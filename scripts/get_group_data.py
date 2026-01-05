import argparse
import csv
import json
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict, Field
from tqdm import tqdm

from lib.clients.vk import VKClient

load_dotenv()

DEFAULT_GROUP_FIELDS = "screen_name,name,is_closed,description,members_count"
DEFAULT_OUT = Path("data/external/group_data.json") # for --group
DEFAULT_OUTDIR = Path("data/external/groups")       # for --csv

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
        for t in self.client.paginate(self.client.api.board.getTopics, group_id=group.id):
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


@dataclass(frozen=True)
class Task:
    place_id: str
    url: str


def read_tasks(csv_path: Path) -> list[Task]:
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        return [Task(place_id=str(row["number"]), url=str(row["link"])) for row in reader]


def write_dump(path: Path, dump: BoardDumpDTO) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(dump.model_dump(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def process_one(service: VKBoardService, url: str, out: Path) -> bool:
    group = service.get_group(url)
    if not group:
        return False
    dump = service.dump_board(group)
    write_dump(out, dump)
    return True


def process_many(service: VKBoardService, csv_path: Path, outdir: Path) -> None:
    tasks = read_tasks(csv_path)

    ok = 0
    skipped = 0

    for t in tqdm(tasks):
        out = outdir / f"place_{t.place_id}.json"
        try:
            if process_one(service, t.url, out):
                ok += 1
            else:
                skipped += 1
        except Exception:
            skipped += 1

    print(f"saved={ok} skipped={skipped} outdir={outdir}")


def main() -> None:
    ap = argparse.ArgumentParser()
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--group")
    src.add_argument("--csv")

    ap.add_argument("--out")
    ap.add_argument("--outdir")
    ap.add_argument("--token", default=None)
    args = ap.parse_args()

    token = (args.token or os.getenv("VK_TOKEN") or "").strip()
    if not token:
        raise ValueError("Got empty token")

    client = VKClient(token)
    service = VKBoardService(client, fields=DEFAULT_GROUP_FIELDS)

    if args.group:
        out = Path(args.out) if args.out else DEFAULT_OUT
        try:
            ok = process_one(service, args.group, out)
        except Exception as e:
            raise SystemExit(f"Error: {e}") from e
        if not ok:
            print("Failed to find group, exit")
            return
        print(f"saved={out}")
        return

    csv_path = Path(args.csv)
    outdir = Path(args.outdir) if args.outdir else DEFAULT_OUTDIR
    process_many(service, csv_path, outdir)


if __name__ == "__main__":
    main()
