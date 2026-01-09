import argparse
import asyncio
from pathlib import Path
from typing import Generator

import litellm
import pandas as pd
from dotenv import load_dotenv
from loguru import logger
from tqdm import tqdm

from src.clients.llm import LLMClient
from src.dto.dto_wishes import NonbookWishExtractionResponse, TopicTextPayload
from src.prompts.extract_nonbooks_wishes import NONBOOK_WISHES_PROMPT
from src.utils import remove_emojis

load_dotenv()

MODEL_NAME = "openai/gpt-5-mini"
TEMPERATURE = 0.0
SEPARATOR = "\n\n---\n\n"

ALLOWED_CATEGORIES = {
    "furniture",
    "tech_equipment",
    "supplies",
    "nonbook_activities",
    "facility_care",
    "event_decor",
    "other",
}


def filter_nonbook_wish_messages(
    topics_df: pd.DataFrame, messages_df: pd.DataFrame
) -> list[TopicTextPayload]:
    """Extract and format messages from nonbook_wish topics."""
    nonbook_topics = topics_df[topics_df["topic_type"] == "nonbook_wish"]
    if nonbook_topics.empty:
        return []

    messages_df["message_text"] = messages_df["message_text"].fillna("").apply(remove_emojis)

    relevant_messages_df = pd.merge(
        messages_df,
        nonbook_topics[["place_id", "group_id", "topic_id", "topic_title"]],
        on=["place_id", "group_id", "topic_id"],
    )

    relevant_messages_df = relevant_messages_df.sort_values(
        ["place_id", "group_id", "topic_id", "message_idx"]
    )

    return [
        TopicTextPayload(
            place_id=int(row.place_id),
            group_id=int(row.group_id),
            topic_id=int(row.topic_id),
            topic_title=str(row.topic_title or ""),
            topic_text=f"[msg {int(row.message_idx)}]\n{row.message_text.strip()}",
        )
        for row in relevant_messages_df.itertuples()
        if row.message_text.strip()
    ]


def create_batches(
    items: list[TopicTextPayload],
    max_chars: int = 5000,
    max_items: int = 10,
) -> Generator[list[TopicTextPayload], None, None]:
    """Batch items respecting character and item limits."""
    current_batch: list[TopicTextPayload] = []
    current_size = 0

    for item in items:
        item_size = len(item.topic_text)

        if current_batch:
            new_size = current_size + len(SEPARATOR) + item_size
            if (max_chars and new_size > max_chars) or (max_items and len(current_batch) >= max_items):
                yield current_batch
                current_batch = []
                current_size = 0

        current_batch.append(item)
        current_size += item_size + (len(SEPARATOR) if current_size > 0 else 0)

    if current_batch:
        yield current_batch


def prepare_batch_payload(batch: list[TopicTextPayload]) -> list[dict]:
    """Convert batch of TopicTextPayload to LLM input format."""
    return [
        {
            "place_id": item.place_id,
            "group_id": item.group_id,
            "topic_id": item.topic_id,
            "topic_title": item.topic_title,
            "topic_text": item.topic_text,
        }
        for item in batch
    ]


def clean_and_deduplicate_results(results: list) -> pd.DataFrame:
    """Clean, filter, and deduplicate extracted nonbook wishes."""
    if not results:
        return pd.DataFrame()

    df = pd.DataFrame([r.model_dump() for r in results])

    df["object_name"] = df["object_name"].fillna("").astype(str).str.strip()
    df["category"] = df["category"].fillna("").astype(str).str.strip()
    df["object_url"] = df["object_url"].fillna("").astype(str).str.strip()
    df["confidence"] = pd.to_numeric(df["confidence"], errors="coerce").fillna(0.0)

    # basic filters
    df = df[df["object_name"] != ""]
    df = df[df["confidence"] >= 0.6]
    df = df[df["category"].isin(ALLOWED_CATEGORIES)]

    if df.empty:
        return df

    df = df.groupby(
        ["place_id", "group_id", "topic_id", "object_name", "category", "object_url"],
        as_index=False,
    )["confidence"].max()

    return df.sort_values(
        ["place_id", "group_id", "topic_id", "category", "object_name", "object_url"]
    )[
        ["place_id", "group_id", "topic_id", "object_name", "category", "object_url", "confidence"]
    ]


def save_empty_csv(output_path: Path) -> None:
    empty_df = pd.DataFrame(
        columns=["place_id", "group_id", "topic_id", "object_name", "category", "object_url", "confidence"]
    )
    empty_df.to_csv(output_path, index=False)


def _write_csv(df: pd.DataFrame, path: Path) -> None:
    df.to_csv(path, index=False)


async def process_batch_async(
    client: LLMClient,
    batch: list[TopicTextPayload],
    batch_num: int,
    total_batches: int,
) -> list:
    payload = prepare_batch_payload(batch)
    total_chars = sum(len(item["topic_text"]) for item in payload)

    logger.info(
        "[batch {}/{}] items={} chars={}",
        batch_num,
        total_batches,
        len(batch),
        total_chars,
    )

    response = await client.structured_call_async(
        response_format=NonbookWishExtractionResponse,
        payload=payload,
        user_prefix="ВХОД:\n",
    )

    # failsafe
    return getattr(response, "rows", []) or []


async def process_and_persist_batch(
    client: LLMClient,
    batch: list[TopicTextPayload],
    batch_num: int,
    total_batches: int,
    batch_dir_path: Path,
) -> list:
    batch_results = await process_batch_async(client, batch, batch_num, total_batches)
    batch_df = await asyncio.to_thread(clean_and_deduplicate_results, batch_results)
    await asyncio.to_thread(_write_csv, batch_df, batch_dir_path / f"{batch_num}.csv")
    return batch_results


async def main_async() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--topics-with-labels-csv", default="data/processed/topics_with_labels.csv")
    parser.add_argument("--messages-csv", default="data/interim/messages.csv")
    parser.add_argument("--out-csv", default="data/processed/wishes_nonbook.csv")
    parser.add_argument("--max-batch-chars", type=int, default=5000)
    parser.add_argument("--max-batch-messages", type=int, default=10)

    parser.add_argument("--max-concurrency", type=int, default=20)
    parser.add_argument("--n-retries", type=int, default=2)
    parser.add_argument("--retry-delay-s", type=float, default=10.0)

    args = parser.parse_args()

    topics_path = Path(args.topics_with_labels_csv)
    messages_path = Path(args.messages_csv)
    output_path = Path(args.out_csv)

    if not topics_path.exists() or not messages_path.exists():
        raise FileNotFoundError("Required input files not found")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    batch_dir_path = output_path.parent / "nonbook_batches"
    batch_dir_path.mkdir(parents=True, exist_ok=True)

    litellm.drop_params = True  # openai APIs could skip temperature

    client = LLMClient(
        model_name=MODEL_NAME,
        temperature=TEMPERATURE,
        system_prompt=NONBOOK_WISHES_PROMPT,
        max_concurrency=args.max_concurrency,
        n_retries=args.n_retries,
        retry_delay_s=args.retry_delay_s,
    )

    topics_df = pd.read_csv(topics_path)
    messages_df = pd.read_csv(messages_path)

    nonbook_messages = filter_nonbook_wish_messages(topics_df, messages_df)
    if not nonbook_messages:
        save_empty_csv(output_path)
        logger.info("Wrote 0 rows -> {}", output_path)
        return

    batches = list(create_batches(nonbook_messages, args.max_batch_chars, args.max_batch_messages))
    logger.info(
        "Found {} messages, grouping into {} batches",
        len(nonbook_messages),
        len(batches),
    )

    tasks: list[asyncio.Task[list]] = []
    total = len(batches)
    for i, batch in enumerate(batches, 1):
        tasks.append(
            asyncio.create_task(process_and_persist_batch(client, batch, i, total, batch_dir_path))
        )

    all_results: list = []
    for fut in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
        try:
            batch_results = await fut
        except Exception:
            logger.exception("Batch task failed unexpectedly")
            continue
        all_results.extend(batch_results)

    final_df = await asyncio.to_thread(clean_and_deduplicate_results, all_results)
    if final_df.empty:
        save_empty_csv(output_path)
    else:
        await asyncio.to_thread(_write_csv, final_df, output_path)

    logger.info("Wrote {} rows -> {}", len(final_df), output_path)


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
