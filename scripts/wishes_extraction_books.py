import argparse
from pathlib import Path
from typing import Generator

import litellm
import pandas as pd
from tqdm import tqdm

from src.dto.dto_wishes import BookWishExtractionResponse, TopicTextPayload
from src.clients.llm import LLMClient
from src.prompts.extract_books_wishes import BOOK_WISH_EXTRACTION_PROMPT
from src.utils import remove_emojis
from dotenv import load_dotenv

load_dotenv()

MODEL_NAME = "openai/gpt-5-mini"
TEMPERATURE = 0.0
SEPARATOR = "\n\n---\n\n"


def filter_book_wish_messages(topics_df: pd.DataFrame, messages_df: pd.DataFrame) -> list[TopicTextPayload]:
    """Extract and format messages from book_wish topics."""
    book_topics = topics_df[topics_df["topic_type"] == "book_wish"]
    if book_topics.empty:
        return []
    
    # Clean and prepare data
    messages_df["message_text"] = messages_df["message_text"].fillna("").apply(remove_emojis)
    
    # Join and filter
    relevant_messages = pd.merge(
        messages_df,
        book_topics[["place_id", "group_id", "topic_id", "topic_title"]],
        on=["place_id", "group_id", "topic_id"]
    )
    
    # Sort and format
    relevant_messages = relevant_messages.sort_values(
        ["place_id", "group_id", "topic_id", "message_idx"]
    )
    
    return [
        TopicTextPayload(
            place_id=int(row.place_id),
            group_id=int(row.group_id),
            topic_id=int(row.topic_id),
            topic_title=str(row.topic_title or ""),
            topic_text=f"[msg {int(row.message_idx)}]\n{row.message_text.strip()}"
        )
        for row in relevant_messages.itertuples()
        if row.message_text.strip()
    ]


def create_batches(
    items: list[TopicTextPayload],
    max_chars: int = 5000,
    max_items: int = 10
) -> Generator[list[TopicTextPayload], None, None]:
    """Batch items respecting character and item limits."""
    current_batch = []
    current_size = 0
    
    for item in items:
        item_size = len(item.topic_text)
        
        # Check if we need to start a new batch
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


def process_batch(client: LLMClient, batch: list[TopicTextPayload], batch_num: int, total_batches: int) -> list:
    """Process a single batch through LLM."""
    payload = prepare_batch_payload(batch)
    total_chars = sum(len(item["topic_text"]) for item in payload)
    
    print(f"[batch {batch_num}/{total_batches}] items={len(batch)} chars={total_chars}")
    
    response = client.structured_call(
        response_format=BookWishExtractionResponse,
        payload=payload,
        user_prefix="ВХОД:\n",
    )
    
    return response.rows


def clean_and_deduplicate_results(results: list) -> pd.DataFrame:
    """Clean, filter, and deduplicate extracted book wishes."""
    if not results:
        return pd.DataFrame()
    
    df = pd.DataFrame([r.model_dump() for r in results])
    
    # Clean text fields
    df["author"] = df["author"].fillna("").str.strip()
    df["book_title"] = df["book_title"].fillna("").str.strip()
    df["confidence"] = pd.to_numeric(df["confidence"], errors="coerce").fillna(0.0)
    
    # Filter empty titles and deduplicate
    df = df[df["book_title"] != ""]
    if df.empty:
        return df
    
    # Keep highest confidence per unique combination
    df = df.groupby(
        ["place_id", "group_id", "topic_id", "author", "book_title"],
        as_index=False
    )["confidence"].max()
    
    return df.sort_values(
        ["place_id", "group_id", "topic_id", "author", "book_title"]
    )[["place_id", "group_id", "topic_id", "author", "book_title", "confidence"]]


def save_empty_csv(output_path: Path) -> None:
    """Save empty CSV with required columns."""
    empty_df = pd.DataFrame(columns=[
        "place_id", "group_id", "topic_id", "author", "book_title", "confidence"
    ])
    empty_df.to_csv(output_path, index=False)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--topics-with-labels-csv", default="data/processed/topics_with_labels.csv")
    parser.add_argument("--messages-csv", default="data/interim/messages.csv")
    parser.add_argument("--out-csv", default="data/processed/wishes_books.csv")
    parser.add_argument("--max-batch-chars", type=int, default=10000)
    parser.add_argument("--max-batch-messages", type=int, default=20)
    
    args = parser.parse_args()
    
    # Validate inputs
    topics_path = Path(args.topics_with_labels_csv)
    messages_path = Path(args.messages_csv)
    output_path = Path(args.out_csv)
    
    if not topics_path.exists() or not messages_path.exists():
        raise FileNotFoundError("Required input files not found")
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    batch_dir_path = output_path.parent / "book_batches"
    batch_dir_path.mkdir(parents=True, exist_ok=True)
    
    litellm.drop_params = True  # openai APIs could skip temperature
    
    # Initialize LLM client
    client = LLMClient(
        model_name=MODEL_NAME,
        temperature=TEMPERATURE,
        system_prompt=BOOK_WISH_EXTRACTION_PROMPT,
    )
    
    # Load and filter data
    topics_df = pd.read_csv(topics_path)
    messages_df = pd.read_csv(messages_path)
    
    book_wish_messages = filter_book_wish_messages(topics_df, messages_df)
    
    if not book_wish_messages:
        save_empty_csv(output_path)
        print(f"Wrote 0 rows -> {output_path}")
        return
    
    # Process in batches
    batches = list(create_batches(book_wish_messages, args.max_batch_chars, args.max_batch_messages))
    print(f"Found {len(book_wish_messages)} messages, grouping into {len(batches)} batches")
    
    all_results = []
    for i, batch in tqdm(enumerate(batches, 1)):
        batch_results = process_batch(client, batch, i, len(batches))
        batch_df = clean_and_deduplicate_results(batch_results)
        batch_df.to_csv(batch_dir_path/f"{i}.csv", index=False)
        
        all_results.extend(batch_results)
    
    # Process and save results
    final_df = clean_and_deduplicate_results(all_results)
    
    if final_df.empty:
        save_empty_csv(output_path)
    else:
        final_df.to_csv(output_path, index=False)
    
    print(f"Wrote {len(final_df)} rows -> {output_path}")


if __name__ == "__main__":
    main()