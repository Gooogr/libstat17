import argparse
import json
from urllib.parse import parse_qs, quote, urlparse

import pandas as pd
import regex
import requests
from bs4 import BeautifulSoup
from bs4.element import AttributeValueList
from pydantic import BaseModel


class Point(BaseModel):
    number: int
    name: str
    lat: float
    lon: float
    link: str | None


MAP_URL = "https://yandex.ru/maps/?l=mrc&ll=53.493467%2C65.320153&mode=usermaps&source=constructorLink&um=constructor%3Abd8db10fcecef60526f6343abd593ecd7f841eff949a8362c457bd23497aa933&z=12.6"
EMPTY_POINT_NAME = "пустой номер"
DEFAULT_SAVING_PATH = "data/external/geo_points.csv"


def extract_constructor_id(maps_url: str) -> str | None:
    um = parse_qs(urlparse(maps_url).query).get("um", [None])[0]
    if not isinstance(um, str) or not um.startswith("constructor:"):
        return None
    return um.split("constructor:", 1)[1]


def build_widget_url(constructor_id: str, lang: str = "ru_RU") -> str:
    um = quote(f"constructor:{constructor_id}")
    return f"https://yandex.com/map-widget/v1/?lang=ru_RU&scroll=true&source=constructor&um={um}"


def extract_user_map_from_html(html: str) -> dict | None:
    m = regex.search(r'"userMap"\s*:\s*(\{(?:[^{}]|(?1))*\})', html or "")
    if not m:
        return None
    try:
        obj = json.loads(m.group(1))
    except Exception:
        return None
    return obj if isinstance(obj, dict) else None


def extract_link_from_html(html: str) -> str | None:
    if not html:
        return None
    soup = BeautifulSoup(html, "html.parser")
    a = soup.select_one('a[href*="vk.com"]') or soup.select_one("a[href]")
    if not a:
        return None

    raw = a.get("href")
    href = " ".join(raw) if isinstance(raw, AttributeValueList) else str(raw or "")
    return href.strip() or None


def extract_link_from_feature(f: dict) -> str | None:
    link = extract_link_from_html(f.get("subtitle") or "")
    if link:
        return link

    content = f.get("content")
    if isinstance(content, dict):
        return extract_link_from_html(content.get("text") or "")
    if isinstance(content, str):
        return extract_link_from_html(content)
    return None


def points_from_user_map(user_map: dict) -> list[Point]:
    feats = user_map.get("features") or []
    if not isinstance(feats, list):
        return []

    valid: list[dict] = []
    for f in feats:
        coords = f.get("coordinates") if isinstance(f, dict) else None
        if isinstance(coords, list) and len(coords) >= 2:
            valid.append(f)

    total = len(valid)
    points: list[Point] = []

    for i, f in enumerate(valid):
        lon, lat = f["coordinates"][:2]
        points.append(
            Point(
                number=total - i,
                name=str(f.get("title") or f.get("caption") or "unknown").strip(),
                lat=float(lat),
                lon=float(lon),
                link=extract_link_from_feature(f),
            )
        )

    return points


def parse_points_from_constructor(maps_url: str) -> list[Point]:
    cid = extract_constructor_id(maps_url)
    if not cid:
        return []

    sess = requests.Session()
    sess.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0",
            "Accept-Language": "ru,en;q=0.8",
            "Accept": "text/html,*/*",
            "Referer": "https://yandex.ru/",
            "Connection": "keep-alive",
        }
    )

    url = build_widget_url(cid)
    try:
        r = sess.get(url, timeout=20)
    except requests.RequestException:
        return []

    user_map = extract_user_map_from_html(r.text)
    if not user_map:
        return []
    places = points_from_user_map(user_map)
    return places


def is_empty_point(point: Point) -> bool:
    if not point.link:
        return True
    if point.name == EMPTY_POINT_NAME:
        return True
    return False


def save_csv(points: list[Point], path: str) -> None:
    df = pd.DataFrame([p.model_dump() for p in points if not is_empty_point(p)])
    df.to_csv(path, index=False)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    points = parse_points_from_constructor(MAP_URL)
    save_csv(points, args.out or DEFAULT_SAVING_PATH)
    print(f"Parsed {len(points)} rows")
