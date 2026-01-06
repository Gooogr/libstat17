import argparse
import json
import os
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm

from src.clients.vk import VKClient
from src.dto_group import BoardWithPlaceId
from src.services.board import VKBoardService  # type: ignore[import-untyped]

load_dotenv()

DEFAULT_OUT = Path("data/external/group_data.json")  # for --group
DEFAULT_OUTDIR = Path("data/external/groups")  # for --csv


@dataclass(frozen=True)
class Task:
    place_id: int
    url: str


def read_tasks(csv_path: Path) -> list[Task]:
    df = pd.read_csv(
        csv_path, usecols=["number", "link"], dtype={"number": int, "link": str}
    )
    return [Task(place_id=id, url=url) for id, url in zip(df.number, df.link)]


def write_dump(path: Path, dump: BoardWithPlaceId) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(dump.model_dump(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def process_one(
    service: VKBoardService, url: str, out: Path, place_id: int | None = None
) -> bool:
    group = service.get_group(url)
    if not group:
        return False
    board = service.dump_board(group)
    place_id = place_id or -1  # we don't have place_id for --group case
    write_dump(out, BoardWithPlaceId(place_id=place_id, board=board))
    return True


def process_many(service: VKBoardService, csv_path: Path, outdir: Path) -> None:
    tasks = read_tasks(csv_path)

    ok = 0
    skipped = 0

    for t in tqdm(tasks):
        out = outdir / f"place_{t.place_id}.json"
        try:
            if process_one(service, t.url, out, t.place_id):
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
    service = VKBoardService(client)

    if args.group:
        out = Path(args.out) if args.out else DEFAULT_OUT
        try:
            ok = process_one(service, url=args.group, out=out)
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
