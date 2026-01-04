import argparse
import json
import os
from pathlib import Path
from urllib.parse import urlencode, urlparse

import vk_api
from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict, Field
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential
from vk_api.exceptions import ApiError

load_dotenv()

VK_API_VERSION = "5.131"
DEFAULT_GROUP_FIELDS = "screen_name,name,is_closed,description,members_count"

PAGE_SIZE = 100
MAX_RETRIES = 10


class GroupDTO(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: int
    name: str | None = None
    screen_name: str | None = None
    members_count: int | None = None
    is_closed: int | None = None
    description: str | None = None


class TopicDTO(BaseModel):
    topic_id: int
    title: str = ""
    messages: list[str] = Field(default_factory=list)


class BoardDumpDTO(BaseModel):
    group: GroupDTO
    topics: list[TopicDTO] = Field(default_factory=list)


def vk_slug(value: str) -> str:
    s = (value or "").strip()
    if not s:
        return ""
    if "://" not in s and ("vk.com/" in s or "vkontakte.ru/" in s):
        s = "https://" + s
    path = urlparse(s).path.strip("/")
    return (path.split("/", 1)[0] if path else s).strip()


def prompt_token() -> str:
    base = "https://oauth.vk.com/authorize"
    params = {
        "client_id": "6121396",
        "redirect_uri": "https://oauth.vk.com/blank.html",
        "response_type": "token",
        "v": VK_API_VERSION,
    }
    print("Access token is required. Open:")
    print(f"{base}?{urlencode(params)}")
    print("After authorization, copy access_token from the address bar (vk1.a...)")
    token = input("access_token: ").strip()
    if not token:
        raise ValueError("Empty token")
    return token


def _is_rate_limit(e: BaseException) -> bool:
    return isinstance(e, ApiError) and getattr(e, "code", None) == 6


@retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=0.35, min=0.35, max=3.0),
    retry=retry_if_exception(_is_rate_limit),
    reraise=True,
)
def vk_call(fn, **params):
    return fn(**params)


def paginate(fn, **params):
    offset = 0
    while True:
        r = vk_call(fn, **params, count=PAGE_SIZE, offset=offset)
        items = r.get("items") or []
        if not items:
            return
        yield from items
        offset += len(items)
        if offset >= int(r.get("count") or 0):
            return


def make_vk(token: str):
    return vk_api.VkApi(token=token, api_version=VK_API_VERSION).get_api()


def get_vk(token: str | None):
    token = (token or os.getenv("VK_TOKEN") or "").strip() or prompt_token()
    vk = make_vk(token)

    try:
        vk_call(vk.users.get)
    except ApiError as e:
        if getattr(e, "code", None) == 5:
            raise ValueError("Invalid token: authorization failed") from e
        raise

    return vk


def fetch_group(vk, group_input: str, fields: str) -> GroupDTO | None:
    slug = vk_slug(group_input)
    if not slug:
        return None
    items = vk_call(vk.groups.getById, group_ids=slug, fields=fields) or []
    return GroupDTO.model_validate(items[0]) if items else None


def fetch_topic_messages(vk, group_id: int, topic_id: int) -> list[str]:
    out: list[str] = []
    for c in paginate(vk.board.getComments, group_id=group_id, topic_id=topic_id):
        text = (c.get("text") or "").strip()
        if text:
            out.append(text)
    return out


def fetch_board_dump(vk, group: GroupDTO) -> BoardDumpDTO:
    topics: list[TopicDTO] = []
    for t in paginate(vk.board.getTopics, group_id=group.id):
        topic_id = int(t["id"])
        title = (t.get("title") or "").strip()
        topics.append(
            TopicDTO(
                topic_id=topic_id,
                title=title,
                messages=fetch_topic_messages(vk, group.id, topic_id),
            )
        )
    return BoardDumpDTO(group=group, topics=topics)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--group", required=True)
    ap.add_argument("--token", default=None)
    ap.add_argument("--fields", default=DEFAULT_GROUP_FIELDS)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    try:
        vk = get_vk(args.token)
        group = fetch_group(vk, args.group, args.fields)
        if not group:
            print("Skip: empty/root VK link")
            return 0
        dump = fetch_board_dump(vk, group)
    except (ApiError, ValueError) as e:
        print(f"Error: {e}")
        return 1

    out = Path(args.out) if args.out else Path(f"vk_board_{dump.group.id}.json")
    out.write_text(
        json.dumps(dump.model_dump(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    total_msgs = sum(len(t.messages) for t in dump.topics)
    print(f"group_id={dump.group.id} topics={len(dump.topics)} messages={total_msgs} saved={out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
