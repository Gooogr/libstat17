import csv
import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Optional
from urllib.parse import urlparse, parse_qs, quote, parse_qsl

import requests
from bs4 import BeautifulSoup


@dataclass
class Place:
    number: int
    name: str
    lat: float
    lon: float
    link: Optional[str]


# ------------------------ logging / debug dumps ------------------------


def _setup_logger() -> logging.Logger:
    logger = logging.getLogger("libra")
    logger.setLevel(logging.DEBUG)

    if logger.handlers:
        return logger

    h = logging.StreamHandler()
    h.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s.%(msecs)03d %(levelname)s %(message)s", datefmt="%H:%M:%S")
    h.setFormatter(fmt)
    logger.addHandler(h)
    return logger


LOG = _setup_logger()
DEBUG_DIR = os.environ.get("DEBUG_DIR", "debug")


def _ensure_debug_dir() -> None:
    os.makedirs(DEBUG_DIR, exist_ok=True)


def _safe_name(s: str, max_len: int = 120) -> str:
    s = re.sub(r"[^a-zA-Z0-9._-]+", "_", s)
    return s[:max_len].strip("_") or "dump"


def _dump_bytes(name: str, data: bytes) -> str:
    _ensure_debug_dir()
    path = os.path.join(DEBUG_DIR, name)
    with open(path, "wb") as f:
        f.write(data)
    return path


def _dump_text(name: str, text: str) -> str:
    return _dump_bytes(name, text.encode("utf-8", errors="replace"))


def _is_captcha_payload(text: str) -> bool:
    """
    IMPORTANT: keep it strict to avoid false positives.
    The widget HTML may mention 'captcha' in unrelated JS/strings while still containing userMap data.
    """
    t = (text or "").lower()
    return (
        "showcaptcha" in t
        or "smartcaptcha" in t
        or "captcha.yandex" in t
        or "/showcaptcha" in t
        or 'action="/showcaptcha"' in t
        or "https://yandex.ru/showcaptcha" in t
        or "https://yandex.com/showcaptcha" in t
    )


def _http_get(
    sess: requests.Session,
    url: str,
    *,
    headers: Optional[dict] = None,
    allow_redirects: bool = True,
    timeout: float = 30.0,
    label: str = "resp",
) -> requests.Response:
    t0 = datetime.now()
    resp = sess.get(url, headers=headers, allow_redirects=allow_redirects, timeout=timeout)
    dt = (datetime.now() - t0).total_seconds()

    ct = resp.headers.get("content-type", "")
    ce = resp.headers.get("content-encoding", "")
    LOG.info(
        "GET %s status=%s elapsed=%.2fs bytes=%s content-type=%s content-encoding=%s redirects=%s",
        url,
        resp.status_code,
        dt,
        len(resp.content or b""),
        ct,
        ce,
        len(resp.history),
    )

    try:
        base = _safe_name(label + "_" + urlparse(url).path.replace("/", "_") + "_" + urlparse(url).query[:80])
        if "text" in ct or "json" in ct or "javascript" in ct or ct == "" or len(resp.content) < 2_000_000:
            _dump_text(f"{base}.txt", resp.text[:5_000_000])
            LOG.debug("Saved text dump: %s/%s.txt", DEBUG_DIR, base)
        else:
            _dump_bytes(f"{base}.bin", resp.content)
            LOG.debug("Saved binary dump: %s/%s.bin", DEBUG_DIR, base)
    except Exception as e:
        LOG.debug("Failed to dump response (%s): %s", url, e)

    return resp


# ------------------------ core logic ------------------------


def extract_constructor_id(maps_url: str) -> str:
    qs = parse_qs(urlparse(maps_url).query)
    um_vals = qs.get("um")
    if not um_vals:
        raise ValueError("No 'um' query param found in the URL")
    um = um_vals[0]
    if not um.startswith("constructor:"):
        raise ValueError(f"Unexpected um format: {um}")
    return um.split("constructor:", 1)[1]


def build_constructor_js_url(constructor_id: str, *, lang: str = "ru_RU") -> str:
    um = f"constructor:{constructor_id}"
    return (
        "https://api-maps.yandex.ru/services/constructor/1.0/js/"
        f"?um={quote(um)}&width=500&height=400&id=mymap&lang={lang}&scroll=true"
    )


def build_constructor_static_url(constructor_id: str, *, lang: str = "ru_RU") -> str:
    um = f"constructor:{constructor_id}"
    return "https://api-maps.yandex.ru/services/constructor/1.0/static/" f"?um={quote(um)}&lang={lang}"


def build_widget_urls(constructor_id: str, *, lang: str = "ru_RU") -> list[str]:
    um = quote(f"constructor:{constructor_id}")
    return [
        f"https://yandex.com/map-widget/v1/?lang={lang}&scroll=true&source=constructor&um={um}",
        f"https://yandex.com/map-widget/v1/?lang={lang}&scroll=true&source=constructor-api&um={um}",
        f"https://yandex.ru/map-widget/v1/?lang={lang}&scroll=true&source=constructor&um={um}",
        f"https://yandex.ru/map-widget/v1/?lang={lang}&scroll=true&source=constructor-api&um={um}",
    ]


def _extract_balanced_json(text: str, start_pos: int) -> Optional[str]:
    if start_pos < 0 or start_pos >= len(text):
        return None
    ch0 = text[start_pos]
    if ch0 not in "{[":
        return None
    open_ch = ch0
    close_ch = "}" if open_ch == "{" else "]"

    depth = 0
    in_str = False
    esc = False
    for i in range(start_pos, len(text)):
        ch = text[i]

        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            continue

        if ch == '"':
            in_str = True
            continue

        if ch == open_ch:
            depth += 1
        elif ch == close_ch:
            depth -= 1
            if depth == 0:
                return text[start_pos : i + 1]

    return None


def _extract_user_map_from_html(html_text: str) -> Optional[dict]:
    if not html_text:
        return None

    m = re.search(r'"userMap"\s*:', html_text)
    if not m:
        LOG.debug('userMap marker not found in HTML')
        return None

    brace = html_text.find("{", m.end())
    if brace < 0:
        LOG.debug("userMap: '{' not found after marker")
        return None

    blob = _extract_balanced_json(html_text, brace)
    if not blob:
        LOG.debug("userMap: failed to extract balanced JSON")
        return None

    try:
        obj = json.loads(blob)
    except Exception as e:
        LOG.debug("userMap: json.loads failed: %s", e)
        _dump_text("userMap_blob_failed.json", blob[:2_000_000])
        return None

    feats = obj.get("features")
    LOG.info("userMap extracted: keys=%s features=%s", list(obj.keys())[:20], len(feats) if isinstance(feats, list) else None)
    return obj


def _extract_link_from_html(html: str) -> Optional[str]:
    if not html:
        return None
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        if "vk.com" in href:
            return href
    a0 = soup.find("a", href=True)
    return (a0["href"].strip() if a0 and a0.get("href") else None)


def _places_from_user_map(user_map: dict) -> list[Place]:
    places: list[Place] = []
    features = user_map.get("features") or []
    if not isinstance(features, list):
        return places

    # 1) сначала отфильтруем только реальные точки с координатами,
    # чтобы нумерация совпадала со списком в интерфейсе карты
    valid_feats: list[dict] = []
    for feat in features:
        if not isinstance(feat, dict):
            continue
        coords = feat.get("coordinates")
        if not (isinstance(coords, list) and len(coords) >= 2):
            continue
        valid_feats.append(feat)

    total = len(valid_feats)
    if total == 0:
        return places

    # 2) у Яндекса нумерация в списке обратная: первый в массиве -> номер total
    for idx, feat in enumerate(valid_feats, start=1):
        coords = feat["coordinates"]
        lon, lat = float(coords[0]), float(coords[1])

        name = (feat.get("title") or feat.get("caption") or "unknown")
        name = str(name).strip()

        subtitle = feat.get("subtitle") or ""
        link = _extract_link_from_html(subtitle)

        if not link:
            content = feat.get("content")
            if isinstance(content, dict):
                link = _extract_link_from_html(content.get("text") or "")
            elif isinstance(content, str):
                link = _extract_link_from_html(content)

        number = total - idx + 1
        places.append(Place(number=number, name=name, lat=lat, lon=lon, link=link))

    return places


def _parse_static_redirect_points(static_redirect_url: str) -> list[Place]:
    q = dict(parse_qsl(urlparse(static_redirect_url).query, keep_blank_values=True))
    pt = q.get("pt") or ""
    if not pt:
        return []

    points = []
    for i, part in enumerate(pt.split("~"), start=1):
        part = part.strip()
        if not part:
            continue
        bits = part.split(",")
        if len(bits) < 2:
            continue
        try:
            lon = float(bits[0])
            lat = float(bits[1])
        except Exception:
            continue
        points.append(Place(number=i, name=f"point_{i}", lat=lat, lon=lon, link=None))
    return points


def parse_places_from_constructor(maps_url: str) -> list[Place]:
    constructor_id = extract_constructor_id(maps_url)
    LOG.info("Constructor id: %s", constructor_id)

    sess = requests.Session()
    headers = {
        "User-Agent": os.environ.get(
            "UA",
            "Mozilla/5.0 (X11; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0",
        ),
        "Accept-Language": "ru,en;q=0.8",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Connection": "keep-alive",
    }

    try:
        _http_get(sess, "https://yandex.com/", headers=headers, timeout=10, label="warmup_yandex_com")
    except Exception as e:
        LOG.debug("Warmup yandex.com failed: %s", e)
    try:
        _http_get(sess, "https://yandex.ru/", headers=headers, timeout=10, label="warmup_yandex_ru")
    except Exception as e:
        LOG.debug("Warmup yandex.ru failed: %s", e)

    widget_urls = build_widget_urls(constructor_id)
    for wurl in widget_urls:
        LOG.info("Trying widget url: %s", wurl)
        try:
            r = _http_get(
                sess,
                wurl,
                headers={**headers, "Referer": "https://yandex.ru/"},
                timeout=30,
                label="widget",
            )
        except Exception as e:
            LOG.warning("Widget fetch failed: %s", e)
            continue

        txt = r.text or ""
        captcha_markers = _is_captcha_payload(txt)

        user_map = _extract_user_map_from_html(txt)
        if user_map:
            places = _places_from_user_map(user_map)
            LOG.info("Extracted points from widget userMap.features: %s (captcha_markers=%s)", len(places), captcha_markers)
            if places:
                return places

        if captcha_markers:
            LOG.warning("CAPTCHA-like markers present AND no userMap parsed; skipping this widget url.")
            continue

        LOG.debug("Widget fetched but userMap not parsed; trying next widget url...")

    LOG.warning("Widget parsing did not yield any userMap.features (maybe blocked / changed payload).")

    js_url = build_constructor_js_url(constructor_id)
    LOG.info("Constructor JS url: %s", js_url)
    try:
        _http_get(sess, js_url, headers=headers, timeout=30, label="constructor_js")
    except Exception as e:
        LOG.warning("Constructor JS fetch failed: %s", e)

    static_url = build_constructor_static_url(constructor_id)
    LOG.info("Constructor STATIC url: %s", static_url)

    try:
        sresp = _http_get(
            sess,
            static_url,
            headers={**headers, "Accept": "*/*"},
            timeout=30,
            allow_redirects=False,
            label="constructor_static",
        )

        loc = sresp.headers.get("Location")
        if not loc:
            body = (sresp.text or "").strip()
            m = re.search(r"(https?://static-maps\.yandex\.(?:ru|com)/1\.x/\?[^\s]+)", body)
            if m:
                loc = m.group(1)

        if not loc and "static-maps.yandex" in (sresp.url or ""):
            loc = sresp.url

        if not loc:
            LOG.error("Static endpoint didn't provide redirect Location (and no redirect URL in body/url).")
            return []

        LOG.info("Static redirect Location: %s", loc)
        places = _parse_static_redirect_points(loc)
        LOG.info("Extracted points from static redirect (pt=): %s", len(places))
        return places

    except Exception as e:
        LOG.error("Static fallback failed: %s", e)
        return []


def save_csv(places: Iterable[Place], path: str) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["number", "name", "lat", "lon", "link"])
        for p in places:
            w.writerow([p.number, p.name, p.lat, p.lon, p.link or ""])


if __name__ == "__main__":
    MAP_URL = "https://yandex.ru/maps/?l=mrc&ll=53.493467%2C65.320153&mode=usermaps&source=constructorLink&um=constructor%3Abd8db10fcecef60526f6343abd593ecd7f841eff949a8362c457bd23497aa933&z=12.6"

    places = parse_places_from_constructor(MAP_URL)
    print(f"Parsed {len(places)} points")
    save_csv(places, "libraries.csv")
    print("Saved to libraries.csv")
