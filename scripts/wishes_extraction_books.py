"""Extract book wishes from messages."""

import argparse
import asyncio
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from src.dto.dto_wishes import BookWishExtractionResponse
from src.prompts.extract_books_wishes import BOOK_WISH_EXTRACTION_PROMPT
from src.services.wish_extractor import BaseWishExtractor, ExtractionConfig
from src.utils import create_llm_client

load_dotenv()

MODEL_NAME = "openai/gpt-5-mini"
TEMPERATURE = 0.0


class BookWishExtractor(BaseWishExtractor[BookWishExtractionResponse]):
    """Extractor for book wishes."""

    def get_response_format(self) -> type[BookWishExtractionResponse]:
        return BookWishExtractionResponse

    def clean_results(self, results: list[BookWishExtractionResponse]) -> pd.DataFrame:
        """Clean and deduplicate book wish results."""
        if not results:
            return pd.DataFrame()

        df = pd.DataFrame([r.model_dump() for r in results])

        # Clean fields
        df["author"] = df["author"].fillna("").str.strip()
        df["book_title"] = df["book_title"].fillna("").str.strip()
        df["confidence"] = pd.to_numeric(df["confidence"], errors="coerce").fillna(0.0)

        # Filter empty titles
        df = df[df["book_title"] != ""]
        if df.empty:
            return df

        # Deduplicate - keep highest confidence
        df = df.groupby(
            ["place_id", "group_id", "topic_id", "author", "book_title"], as_index=False
        )["confidence"].max()

        return df.sort_values(
            ["place_id", "group_id", "topic_id", "author", "book_title"]
        )

    def get_output_columns(self) -> list[str]:
        return [
            "place_id",
            "group_id",
            "topic_id",
            "author",
            "book_title",
            "confidence",
        ]


async def main_async() -> None:
    """Async main function."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--topics-with-labels-csv", default="data/processed/topics_with_labels.csv"
    )
    parser.add_argument("--messages-csv", default="data/interim/messages.csv")
    parser.add_argument("--out-csv", default="data/processed/wishes_books.csv")
    parser.add_argument("--max-batch-chars", type=int, default=5000)
    parser.add_argument("--max-batch-messages", type=int, default=10)
    parser.add_argument("--max-concurrency", type=int, default=20)
    parser.add_argument("--n-retries", type=int, default=2)
    parser.add_argument("--retry-delay-s", type=float, default=10.0)

    args = parser.parse_args()

    # Validate inputs
    topics_path = Path(args.topics_with_labels_csv)
    messages_path = Path(args.messages_csv)
    output_path = Path(args.out_csv)

    if not topics_path.exists() or not messages_path.exists():
        raise FileNotFoundError("Required input files not found")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Create client and extractor
    client = create_llm_client(
        model_name=MODEL_NAME,
        temperature=TEMPERATURE,
        system_prompt=BOOK_WISH_EXTRACTION_PROMPT,
        max_concurrency=args.max_concurrency,
        n_retries=args.n_retries,
        retry_delay_s=args.retry_delay_s,
    )

    config = ExtractionConfig(
        topic_type="book_wish",
        system_prompt=BOOK_WISH_EXTRACTION_PROMPT,
        output_path=output_path,
        max_batch_chars=args.max_batch_chars,
        max_batch_items=args.max_batch_messages,
        max_concurrency=args.max_concurrency,
        n_retries=args.n_retries,
        retry_delay_s=args.retry_delay_s,
        batch_subdir="book_batches",
    )

    extractor = BookWishExtractor(config, client)

    # Load data
    topics_df = pd.read_csv(topics_path)
    messages_df = pd.read_csv(messages_path)

    # Filter messages
    book_messages = extractor.filter_messages_by_type(topics_df, messages_df)

    if not book_messages:
        extractor.save_empty_output(output_path)
        return

    # Process all batches
    result_df = await extractor.process_all_batches(book_messages, output_path)

    # Save final results
    if result_df.empty:
        extractor.save_empty_output(output_path)
    else:
        result_df.to_csv(output_path, index=False)

    print(f"Wrote {len(result_df)} rows -> {output_path}")


def main() -> None:
    """Main entry point."""
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
