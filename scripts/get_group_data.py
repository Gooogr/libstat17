import argparse
import json
import os
from pathlib import Path

from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict, Field

from lib.clients.vk import VKClient

load_dotenv()

DEFAULT_GROUP_FIELDS = "screen_name,name,is_closed,description,members_count"


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


class VKBoardService:
    def __init__(self, client: VKClient, fields: str = DEFAULT_GROUP_FIELDS):
        self.client = client
        self.fields = fields

    def get_group(self, group_input: str) -> GroupDTO | None:
        slug = self.client.slug(group_input)
        if not slug:
            return None
        items = (
            self.client.call(
                self.client.api.groups.getById, group_ids=slug, fields=self.fields
            )
            or []
        )
        return GroupDTO.model_validate(items[0]) if items else None

    def get_topic_messages(self, group_id: int, topic_id: int) -> list[str]:
        out: list[str] = []
        for c in self.client.paginate(
            self.client.api.board.getComments, group_id=group_id, topic_id=topic_id
        ):
            text = (c.get("text") or "").strip()
            if text:
                out.append(text)
        return out

    def dump_board(self, group: GroupDTO) -> BoardDumpDTO:
        topics: list[TopicDTO] = []
        for t in self.client.paginate(
            self.client.api.board.getTopics, group_id=group.id
        ):
            topic_id = int(t["id"])
            title = (t.get("title") or "").strip()
            topics.append(
                TopicDTO(
                    topic_id=topic_id,
                    title=title,
                    messages=self.get_topic_messages(group.id, topic_id),
                )
            )
        return BoardDumpDTO(group=group, topics=topics)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--group", required=True)
    ap.add_argument("--token", default=None)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    token = args.token or os.getenv("VK_TOKEN", "")

    token = (args.token or os.getenv("VK_TOKEN") or "").strip()
    if not token:
        raise ValueError("Got empty token")

    client = VKClient(token)
    service = VKBoardService(client, fields=DEFAULT_GROUP_FIELDS)
    group = service.get_group(args.group)
    if not group:
        print("Failed to find group, exit")
        return

    dump = service.dump_board(group)

    out = Path(args.out) if args.out else Path(f"vk_board_{dump.group.id}.json")
    out.write_text(
        json.dumps(dump.model_dump(), ensure_ascii=False, indent=2), encoding="utf-8"
    )

    total_msgs = sum(len(t.messages) for t in dump.topics)
    print(
        f"group_id={dump.group.id} topics={len(dump.topics)} messages={total_msgs} saved={out}"
    )


if __name__ == "__main__":
    main()
