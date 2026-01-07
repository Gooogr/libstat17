import argparse
from itertools import islice
from pathlib import Path
from typing import Any, Literal

import litellm
import pandas as pd
from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict, Field

from src.clients.llm import LLMClient
from src.dto_group import BoardWithPlaceId
from src.prompts.topic_labeling import TOPIC_LABELING_PROMPT

load_dotenv()

MODEL_NAME = "openai/gpt-5-mini"
TEMPERATURE = 0.0


# --- Input payload model --- #
class TopicsPayload(BaseModel):
    place_id: int
    topics: list[dict[str, Any]]


# --- Output response model --- #
TopicType = Literal["book_wish", "nonbook_wish", "thank", "other"]


class TopicLabelRow(BaseModel):
    model_config = ConfigDict(extra="forbid")

    place_id: int
    topic_id: int
    topic_type: TopicType
    confidence: float = Field(..., ge=0.0, le=1.0)


class TopicLabelingResponse(BaseModel):
    rows: list[TopicLabelRow]


def _parse_board_file(path: Path) -> BoardWithPlaceId:
    return BoardWithPlaceId.model_validate_json(path.read_text(encoding="utf-8"))


def _board_to_payload(board: BoardWithPlaceId) -> TopicsPayload:
    topics = [
        {"topic_id": int(t.topic_id), "topic_title": (t.title or "").strip()}
        for t in board.board.topics
    ]
    return TopicsPayload(place_id=board.place_id, topics=topics)


def load_payload(path: Path) -> TopicsPayload:
    board = _parse_board_file(path)
    return _board_to_payload(board)


def get_sorted_json_files(directory: Path) -> list[Path]:
    return sorted(directory.glob("*.json"))


def chunk_places(places: list[TopicsPayload], max_places: int = 25):
    """Yield successive max_places-sized chunks from places."""
    it = iter(places)
    chunk = list(islice(it, max_places))
    while chunk:
        yield chunk
        chunk = list(islice(it, max_places))


def validate_directory(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Directory does not exist: {path}")
    if not path.is_dir():
        raise NotADirectoryError(f"Not a directory: {path}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-dir", required=True)
    ap.add_argument("--out-csv", required=True)
    ap.add_argument("--max-places", type=int, default=25)
    args = ap.parse_args()

    in_dir = Path(args.in_dir)
    validate_directory(in_dir)

    out_csv = Path(args.out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    litellm.drop_params = True

    client = LLMClient(
        model_name=MODEL_NAME,
        temperature=TEMPERATURE,
        system_prompt=TOPIC_LABELING_PROMPT,
    )

    files = get_sorted_json_files(in_dir)
    if not files:
        raise SystemExit(f"No .json files found in: {in_dir}")

    places = [load_payload(fp) for fp in files]
    places.sort(key=lambda p: p.place_id)

    all_rows: list[TopicLabelRow] = []
    batches = list(chunk_places(places, max_places=args.max_places))

    for i, batch in enumerate(batches, start=1):
        total_topics = sum(len(p.topics) for p in batch)
        print(f"[batch {i}/{len(batches)}] places={len(batch)} topics={total_topics}")

        resp = client.structured_call(
            response_format=TopicLabelingResponse,
            payload=[b.model_dump() for b in batch],
            user_prefix="ВХОД:\n",
        )
        all_rows.extend(resp.rows)

    df = pd.DataFrame([row.model_dump() for row in all_rows])

    if not df.empty:
        df = df.sort_values(["place_id", "topic_id"])

    df.to_csv(out_csv, index=False)
    print(f"Wrote {len(df)} rows -> {out_csv}")


if __name__ == "__main__":
    main()
