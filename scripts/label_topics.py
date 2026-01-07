import argparse
from pathlib import Path
from typing import Any, Iterable, Literal

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, RootModel
import litellm

from src.clients.llm import LLMClient
from src.dto_group import BoardWithPlaceId
from src.prompts.topic_labeling import TOPIC_LABELING_PROMPT

litellm.drop_params = True

MODEL_NAME = "openai/gpt-5-mini"
TEMPERATURE = 0.0


class TopicsPayload(BaseModel):
    place_id: int
    topics: list[dict[str, Any]]  # [{"topic_id": ..., "topic_title": ...}, ...]


TopicType = Literal["book_wish", "nonbook_wish", "thank", "other"]


class TopicLabelRow(BaseModel):
    model_config = ConfigDict(extra="forbid")

    place_id: int
    topic_id: int
    topic_type: TopicType
    confidence: float = Field(..., ge=0.0, le=1.0)


class TopicLabelingResponse(BaseModel):
    rows: list[TopicLabelRow]


def load_payload(path: Path) -> TopicsPayload:
    raw = path.read_text(encoding="utf-8")
    board_with_place = BoardWithPlaceId.model_validate_json(raw)

    topics = []
    for t in board_with_place.board.topics:
        topics.append(
            {"topic_id": int(t.topic_id), "topic_title": (t.title or "").strip()}
        )

    return TopicsPayload(place_id=board_with_place.place_id, topics=topics)


def iter_json_files(in_dir: Path) -> list[Path]:
    return sorted([p for p in in_dir.glob("*.json") if p.is_file()])


def batch_by_place(
    places: list[TopicsPayload],
    *,
    max_places: int = 25,
) -> Iterable[list[TopicsPayload]]:
    batch: list[TopicsPayload] = []
    n_places = 0

    for p in places:
        if batch and (n_places + 1 > max_places):
            yield batch
            batch = []
            n_places = 0

        batch.append(p)
        n_places += 1

    if batch:
        yield batch


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-dir", required=True)
    ap.add_argument("--out-csv", required=True)

    ap.add_argument("--max-places", type=int, default=25)

    args = ap.parse_args()

    in_dir = Path(args.in_dir)
    out_csv = Path(args.out_csv)

    files = iter_json_files(in_dir)
    if not files:
        raise SystemExit(f"No .json files found in: {in_dir}")

    places = [load_payload(fp) for fp in files]
    places.sort(key=lambda p: p.place_id)

    client = LLMClient(
        model_name=MODEL_NAME,
        temperature=TEMPERATURE,
        system_prompt=TOPIC_LABELING_PROMPT,
    )

    all_rows: list[TopicLabelRow] = []

    batches: list[list[TopicsPayload]] = list(
        batch_by_place(
            places,
            max_places=args.max_places,
        )
    )

    for i, batch in enumerate(batches, start=1):
        print(
            f"[batch {i}/{len(batches)}] places={len(batch)} topics={sum(len(p.topics) for p in batch)}"
        )
        resp = client.structured_call(
            response_format=TopicLabelingResponse,
            payload=[b.model_dump() for b in batch],
            user_prefix="ВХОД:\n",
        )
        all_rows.extend(resp.rows)

    df = pd.DataFrame(
        [
            {
                "place_id": r.place_id,
                "topic_id": r.topic_id,
                "topic_type": r.topic_type,
                "confidence": float(r.confidence),
            }
            for r in all_rows
        ]
    )

    if not df.empty:
        df = df.sort_values(["place_id", "topic_id"])

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_csv, index=False)
    print(f"Wrote {len(df)} rows -> {out_csv}")

if __name__ == "__main__":
    main()
    