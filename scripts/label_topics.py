import argparse
from itertools import islice
from pathlib import Path
from typing import Any, Literal

import litellm
import pandas as pd
from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict, Field

from src.clients.llm import LLMClient
from src.prompts.topic_labeling import TOPIC_LABELING_PROMPT
from src.utils import remove_emojis

load_dotenv()

MODEL_NAME = "openai/gpt-5-mini"
TEMPERATURE = 0.0

# Safety to reduce prompt bloat from extremely long first messages
MAX_FIRST_MESSAGE_CHARS = 200


# --- Input payload model --- #
class TopicsPayload(BaseModel):
    place_id: int
    topics: list[dict[str, Any]]  # each dict has: topic_id, topic_title, first_message


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


def batch_payload(places: list[TopicsPayload], max_places: int = 25):
    it = iter(places)
    chunk = list(islice(it, max_places))
    while chunk:
        yield chunk
        chunk = list(islice(it, max_places))




def load_topics_with_first_message(topics_df: pd.DataFrame, messages_df: pd.DataFrame) -> pd.DataFrame:
    topics_df["topic_title"] = topics_df["topic_title"].fillna("").astype(str).str.strip()

    # Get first message per (place_id, topic_id) by smallest message_idx
    first_df = (
        messages_df.sort_values(["place_id", "topic_id", "message_idx"])
        .groupby(["place_id", "topic_id"], as_index=False)
        .first()[["place_id", "topic_id", "message_text"]]
        .rename(columns={"message_text": "first_message"})
    )
    first_df["first_message"] = first_df["first_message"].fillna("").astype(str).apply(remove_emojis)

    # Left join so topics without messages still exist
    out = topics_df.merge(first_df, on=["place_id", "topic_id"], how="left")
    out["first_message"] = out["first_message"].fillna("")

    # Truncate for payload safety (keeps behavior stable; avoids context blowups)
    out["first_message"] = out["first_message"].map(lambda s: s[:MAX_FIRST_MESSAGE_CHARS])

    # Stable ordering
    out = out.sort_values(["place_id", "topic_id"]).reset_index(drop=True)
    return out


def build_place_payloads(df: pd.DataFrame) -> list[TopicsPayload]:
    places: list[TopicsPayload] = []

    df = df.dropna(subset=["place_id", "topic_id"]).copy()
    df["place_id"] = df["place_id"].astype(int)
    df["topic_id"] = df["topic_id"].astype(int)

    for place_id, g in df.groupby("place_id", sort=True):
        g = g.sort_values("topic_id")
        topics = [
            {
                "topic_id": int(r.topic_id),
                "topic_title": str(r.topic_title),
                "first_message": str(r.first_message),
            }
            for r in g.itertuples(index=False)
        ]
        places.append(TopicsPayload(place_id=int(place_id), topics=topics))

    places.sort(key=lambda p: p.place_id)
    return places


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--topics-csv", default="data/interim/topics.csv")
    ap.add_argument("--messages-csv", default="data/interim/messages.csv")
    ap.add_argument("--out-csv", default="data/processed/topics_with_labels.csv")
    ap.add_argument("--max-places", type=int, default=25)
    args = ap.parse_args()

    topics_csv = Path(args.topics_csv)
    messages_csv = Path(args.messages_csv)
    
    if not topics_csv.exists():
        raise FileNotFoundError(f"topics.csv not found: {topics_csv}")
    if not messages_csv.exists():
        raise FileNotFoundError(f"messages.csv not found: {messages_csv}")

    out_csv = Path(args.out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    litellm.drop_params = True  # openai APIs could skip temperature

    client = LLMClient(
        model_name=MODEL_NAME,
        temperature=TEMPERATURE,
        system_prompt=TOPIC_LABELING_PROMPT,
    )
    topics_df = pd.read_csv(topics_csv)
    messages_df = pd.read_csv(messages_csv)

    enriched_df = load_topics_with_first_message(topics_df, messages_df)
    if enriched_df.empty:
        raise SystemExit("No topics to label (topics.csv is empty after loading).")

    places = build_place_payloads(enriched_df)
    if not places:
        raise SystemExit("No places to label (no valid place_id/topic_id rows).")

    all_rows: list[TopicLabelRow] = []
    batches = list(batch_payload(places, max_places=args.max_places))

    for i, batch in enumerate(batches, start=1):
        total_topics = sum(len(p.topics) for p in batch)
        print(f"[batch {i}/{len(batches)}] places={len(batch)} topics={total_topics}")

        resp = client.structured_call(
            response_format=TopicLabelingResponse,
            payload=[b.model_dump() for b in batch],
            user_prefix="ВХОД:\n",
        )
        all_rows.extend(resp.rows)

    topic_labels_df = pd.DataFrame([row.model_dump() for row in all_rows])
    if not topic_labels_df.empty:
        topic_labels_df = topic_labels_df.sort_values(["place_id", "topic_id"])
        
    df_out = topics_df.merge(topic_labels_df, on=["place_id", "topic_id"], how="left")

    df_out.to_csv(out_csv, index=False)
    print(f"Wrote {len(df_out)} rows -> {out_csv}")


if __name__ == "__main__":
    main()
