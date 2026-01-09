import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Generator, Generic, TypeVar

import pandas as pd
from loguru import logger
from pydantic import BaseModel
from tqdm import tqdm

from src.clients.llm import LLMClient
from src.dto.dto_wishes import TopicTextPayload
from src.utils import remove_emojis

T = TypeVar("T", bound=BaseModel)


@dataclass
class ExtractionConfig:
    """Configuration for wish extraction."""

    topic_type: str
    system_prompt: str
    output_path: Path
    max_batch_chars: int = 5000
    max_batch_items: int = 10
    max_concurrency: int = 20
    n_retries: int = 2
    retry_delay_s: float = 10.0
    batch_subdir: str = "batches"


class BaseWishExtractor(ABC, Generic[T]):
    """Base class for extracting wishes from messages."""

    SEPARATOR = "\n\n---\n\n"

    def __init__(self, config: ExtractionConfig, client: LLMClient):
        self.config = config
        self.client = client

    def filter_messages_by_type(
        self, topics_df: pd.DataFrame, messages_df: pd.DataFrame
    ) -> list[TopicTextPayload]:
        """Extract and format messages from topics of specified type."""
        filtered_topics = topics_df[topics_df["topic_type"] == self.config.topic_type]
        if filtered_topics.empty:
            return []

        # Clean message text
        messages_df = messages_df.copy()
        messages_df["message_text"] = (
            messages_df["message_text"].fillna("").apply(remove_emojis)
        )

        # Join with topic data
        relevant_messages = pd.merge(
            messages_df,
            filtered_topics[["place_id", "group_id", "topic_id", "topic_title"]],
            on=["place_id", "group_id", "topic_id"],
            how="inner",
        ).sort_values(["place_id", "group_id", "topic_id", "message_idx"])

        # Format as TopicTextPayload
        return [
            TopicTextPayload(
                place_id=int(row.place_id),
                group_id=int(row.group_id),
                topic_id=int(row.topic_id),
                topic_title=str(row.topic_title or ""),
                topic_text=f"[msg {int(row.message_idx)}]\n{row.message_text.strip()}",
            )
            for row in relevant_messages.itertuples()
            if row.message_text.strip()
        ]

    def create_batches(
        self,
        items: list[TopicTextPayload],
    ) -> Generator[list[TopicTextPayload], None, None]:
        """Batch items respecting character and item limits."""
        current_batch: list[TopicTextPayload] = []
        current_size = 0

        for item in items:
            item_size = len(item.topic_text)

            if current_batch:
                new_size = current_size + len(self.SEPARATOR) + item_size
                exceeds_chars = (
                    self.config.max_batch_chars
                    and new_size > self.config.max_batch_chars
                )
                exceeds_items = (
                    self.config.max_batch_items
                    and len(current_batch) >= self.config.max_batch_items
                )

                if exceeds_chars or exceeds_items:
                    yield current_batch
                    current_batch = []
                    current_size = 0

            current_batch.append(item)
            current_size += item_size + (len(self.SEPARATOR) if current_size > 0 else 0)

        if current_batch:
            yield current_batch

    def prepare_batch_payload(
        self, batch: list[TopicTextPayload]
    ) -> list[dict[str, Any]]:
        """Convert batch to LLM input format."""
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

    async def process_batch_async(
        self,
        batch: list[TopicTextPayload],
        batch_num: int,
        total_batches: int,
    ) -> list[T]:
        """Process a single batch through LLM."""
        payload = self.prepare_batch_payload(batch)
        total_chars = sum(len(item["topic_text"]) for item in payload)

        logger.info(
            f"[batch {batch_num}/{total_batches}] items={len(batch)} chars={total_chars}"
        )

        response = await self.client.structured_call_async(
            response_format=self.get_response_format(),
            payload=payload,
            user_prefix="ВХОД:\n",
        )

        return getattr(response, "rows", []) or []

    async def process_and_persist_batch(
        self,
        batch: list[TopicTextPayload],
        batch_num: int,
        total_batches: int,
        batch_dir: Path,
    ) -> list[T]:
        """Process batch and save intermediate results."""
        batch_results = await self.process_batch_async(batch, batch_num, total_batches)
        batch_df = await asyncio.to_thread(self.clean_results, batch_results)

        if not batch_df.empty:
            await asyncio.to_thread(
                batch_df.to_csv, batch_dir / f"{batch_num}.csv", index=False
            )

        return batch_results

    async def process_all_batches(
        self,
        messages: list[TopicTextPayload],
        output_path: Path,
    ) -> pd.DataFrame:
        """Process all batches and return final DataFrame."""
        batches = list(self.create_batches(messages))

        if not batches:
            logger.info(f"No {self.config.topic_type} messages found")
            return pd.DataFrame()

        logger.info(
            f"Found {len(messages)} messages, grouping into {len(batches)} batches"
        )

        # Create batch directory
        batch_dir = output_path.parent / f"{self.config.batch_subdir}"
        batch_dir.mkdir(parents=True, exist_ok=True)

        # Process batches concurrently
        tasks: list[asyncio.Task[list[T]]] = []
        for i, batch in enumerate(batches, 1):
            task = asyncio.create_task(
                self.process_and_persist_batch(batch, i, len(batches), batch_dir)
            )
            tasks.append(task)

        # Collect results
        all_results: list[T] = []
        for future in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
            try:
                batch_results = await future
                all_results.extend(batch_results)
            except Exception:
                logger.exception("Batch processing failed")

        # Combine and deduplicate results
        return await asyncio.to_thread(self.clean_results, all_results)

    def save_empty_output(self, output_path: Path) -> None:
        """Save empty CSV with appropriate columns."""
        empty_df = pd.DataFrame(columns=self.get_output_columns())
        empty_df.to_csv(output_path, index=False)

    @abstractmethod
    def get_response_format(self) -> type[T]:
        """Return the Pydantic model for LLM response."""
        pass

    @abstractmethod
    def clean_results(self, results: list[T]) -> pd.DataFrame:
        """Clean and deduplicate extracted results."""
        pass

    @abstractmethod
    def get_output_columns(self) -> list[str]:
        """Return column names for output CSV."""
        pass
