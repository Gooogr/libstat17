import argparse
import os
from pathlib import Path
from typing import NamedTuple

import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm

from src.clients.vk import VKClient
from src.dto.dto_group import BoardWithPlaceId
from src.services.board import VKBoardService

load_dotenv()

DEFAULT_SINGLE_OUTPUT = Path("data/external/group_data.json")
DEFAULT_BATCH_OUTPUT_DIR = Path("data/external/groups")


class GroupTask(NamedTuple):
    place_id: int
    url: str


def load_tasks(csv_path: Path) -> list[GroupTask]:
    df = pd.read_csv(csv_path, usecols=["number", "link"])
    return [GroupTask(row.number, row.link) for _, row in df.iterrows()]


def fetch_board(
    service: VKBoardService, url: str, place_id: int = -1
) -> BoardWithPlaceId | None:
    group = service.get_group(url)
    if not group:
        return None
    board = service.dump_board(group)
    return BoardWithPlaceId(place_id=place_id, board=board)


def save_board(board: BoardWithPlaceId, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        board.model_dump_json(indent=2, ensure_ascii=False), encoding="utf-8"
    )


def process_batch(
    service: VKBoardService, tasks: list[GroupTask], output_dir: Path
) -> tuple[int, int]:
    success_count = 0
    failure_count = 0

    for task in tqdm(tasks):
        output_path = output_dir / f"place_{task.place_id}.json"
        board = fetch_board(service, task.url, task.place_id)
        if board:
            save_board(board, output_path)
            success_count += 1
        else:
            failure_count += 1

    return success_count, failure_count


def get_auth_token(token_arg: str | None) -> str:
    token = (token_arg or os.getenv("VK_TOKEN") or "").strip()
    if not token:
        raise ValueError("VK API token is required")
    return token


def main() -> None:
    parser = argparse.ArgumentParser()
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument("--group", help="Process single group by URL")
    mode_group.add_argument("--csv", help="Process multiple groups from CSV")

    parser.add_argument("--out", help="Output file for single group")
    parser.add_argument("--outdir", help="Output directory for batch processing")
    parser.add_argument("--token", help="VK API token (or use VK_TOKEN env var)")

    args = parser.parse_args()
    token = get_auth_token(args.token)
    service = VKBoardService(VKClient(token))

    if args.group:
        output_path = Path(args.out) if args.out else DEFAULT_SINGLE_OUTPUT
        board = fetch_board(service, args.group)
        if board:
            save_board(board, output_path)
            print(f"Saved group data to: {output_path}")
        else:
            print("Failed to fetch group data")
    else:
        output_dir = Path(args.outdir) if args.outdir else DEFAULT_BATCH_OUTPUT_DIR
        tasks = load_tasks(Path(args.csv))
        success, failure = process_batch(service, tasks, output_dir)
        print(f"Processed {len(tasks)} groups: {success} successful, {failure} failed")


if __name__ == "__main__":
    main()
