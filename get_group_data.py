import argparse
import os
from typing import Optional
from urllib.parse import urlencode, urlparse

import requests
import vk_api
from dotenv import load_dotenv
from vk_api.exceptions import ApiError

load_dotenv()

VK_API_VERSION = "5.131"
DEFAULT_GROUP_FIELDS = "screen_name,name,is_closed,description,members_count"


def extract_vk_slug(url: str) -> str:
    return urlparse(url.strip()).path.strip("/").split("/", 1)[0]


def prompt_token() -> str:
    base = "https://oauth.vk.com/authorize"
    params = {
        "client_id": "6121396",
        "redirect_uri": "https://oauth.vk.com/blank.html",
        "response_type": "token",
        "v": VK_API_VERSION,
    }
    print("Access token is required.")
    print(f"{base}?{urlencode(params)}")
    print("After authorization, copy access_token from the browser address bar (vk1.a...)")
    token = input("access_token: ").strip()
    if not token:
        raise ValueError("Empty token")
    return token


def ensure_token(token: Optional[str]) -> str:
    token = (token or "").strip()
    if not token:
        token = prompt_token()

    r = requests.get(
        "https://api.vk.com/method/users.get",
        params={"access_token": token, "v": VK_API_VERSION},
        timeout=30,
    ).json()
    if "error" in r:
        raise ValueError(f"Token is invalid: {r['error'].get('error_msg', r['error'])}")
    return token


def make_vk(token: str):
    return vk_api.VkApi(token=token, api_version=VK_API_VERSION).get_api()


def resolve_group_info(vk, group_url: str, fields: str) -> Optional[dict]:
    slug = extract_vk_slug(group_url)
    if not slug:
        return None
    items = vk.groups.getById(group_ids=slug, fields=fields)
    return items[0] if items else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--group", required=True)
    ap.add_argument("--token", default=os.getenv("VK_TOKEN"))
    ap.add_argument("--fields", default=DEFAULT_GROUP_FIELDS)
    args = ap.parse_args()

    try:
        token = ensure_token(args.token)
        vk = make_vk(token)
        info = resolve_group_info(vk, args.group, args.fields)
    except (ApiError, requests.RequestException, ValueError) as e:
        raise SystemExit(f"Error: {e}") from e

    if not info:
        print("Skip: empty/root VK link")
        return

    print("Resolved group_id:", info.get("id"))
    print("Name:", info.get("name"))
    print("Screen name:", info.get("screen_name"))
    print("Members:", info.get("members_count"))
    print("Is closed:", info.get("is_closed"))


if __name__ == "__main__":
    main()
