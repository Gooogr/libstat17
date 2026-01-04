import json
from urllib.parse import parse_qs, quote, urlparse

import pandas as pd
import regex
import requests
from bs4 import BeautifulSoup
from bs4.element import AttributeValueList
from pydantic import BaseModel


class Place(BaseModel):
    number: int
    name: str
    lat: float
    lon: float
    link: str | None
    
MAP_URL = "https://yandex.ru/maps/?l=mrc&ll=53.493467%2C65.320153&mode=usermaps&source=constructorLink&um=constructor%3Abd8db10fcecef60526f6343abd593ecd7f841eff949a8362c457bd23497aa933&z=12.6"



def extract_constructor_id(maps_url: str) -> str | None:
    um = parse_qs(urlparse(maps_url).query).get("um", [None])[0]
    if not isinstance(um, str) or not um.startswith("constructor:"):
        return None
    return um.split("constructor:", 1)[1]


def build_widget_urls(constructor_id: str, lang: str = "ru_RU") -> list[str]:
    um = quote(f"constructor:{constructor_id}")
    out: list[str] = []
    for domain in ("yandex.com", "yandex.ru"):
        for source in ("constructor", "constructor-api"):
            out.append(
                f"https://{domain}/map-widget/v1/?lang={lang}&scroll=true&source={source}&um={um}"
            )
    return out


def extract_user_map_from_html(html: str) -> dict | None:
    # balanced JSON object after "userMap":
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
    
    raw = a.get("href", "")
    href = " ".join(raw) if isinstance(raw, AttributeValueList) else str(raw)
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


def places_from_user_map(user_map: dict) -> list[Place]:
    feats = user_map.get("features") or []
    if not isinstance(feats, list):
        return []

    valid: list[dict] = []
    for f in feats:
        if not isinstance(f, dict):
            continue
        coords = f.get("coordinates")
        if isinstance(coords, list) and len(coords) >= 2:
            valid.append(f)

    total = len(valid)
    places: list[Place] = []

    # Yandex list numbering is reversed: first feature == number total
    for idx, f in enumerate(valid, start=1):
        coords = f["coordinates"]
        lon, lat = float(coords[0]), float(coords[1])
        name = str(f.get("title") or f.get("caption") or "unknown").strip()
        link = extract_link_from_feature(f)
        number = total - idx + 1
        places.append(Place(number=number, name=name, lat=lat, lon=lon, link=link))

    return places


def parse_places_from_constructor(maps_url: str) -> list[Place]:
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

    for url in build_widget_urls(cid):
        try:
            r = sess.get(url, timeout=20)
        except requests.RequestException:
            continue

        text = r.text or ""
        user_map = extract_user_map_from_html(text)
        if user_map:
            places = places_from_user_map(user_map)
            if places:
                return places

    return []


def save_csv(places: list[Place], path: str) -> None:
    df = pd.DataFrame([p.model_dump() for p in places], columns=["number", "name", "lat", "lon", "link"])
    df.fillna("").to_csv(path, index=False)


if __name__ == "__main__":
    places = parse_places_from_constructor(MAP_URL)
    save_csv(places, "libraries.csv")
    print(f"Parsed {len(places)} rows")