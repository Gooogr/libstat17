import argparse
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, NamedTuple, Optional

import pandas as pd

from src.dto.dto_group import BoardWithPlaceId


logger = logging.getLogger(__name__)

PLACES_COLUMNS = [
    "place_id",
    "place_name",
    "lat",
    "lon",
    "vk_url",
    "group_id",
    "group_name",
    "screen_name",
    "members_count",
    "description",
]

TOPICS_COLUMNS = [
    "place_id",
    "group_id",
    "topic_id",
    "topic_title",
    "message_count",
]

MESSAGES_COLUMNS = [
    "place_id",
    "group_id",
    "topic_id",
    "message_idx",
    "message_text",
]


@dataclass
class Config:
    """Configuration for the flattening process."""
    geo_csv_path: Path
    groups_dir: Path
    output_dir: Path = Path("data/interim")
    log_level: str = "INFO"


class FlattenedData(NamedTuple):
    """Container for flattened data from a single place."""
    place_data: dict[str, Any]
    topics_data: list[dict[str, Any]]
    messages_data: list[dict[str, Any]]


def setup_config() -> Config:
    """Parse command line arguments and create configuration."""
    parser = argparse.ArgumentParser(
        description="Flatten VK group JSONs into CSV files using Pydantic models."
    )
    parser.add_argument(
        "--geo-csv",
        type=Path,
        default=Path("data/external/map_points.csv"),
        help="Path to geo CSV file",
    )
    parser.add_argument(
        "--groups-dir",
        type=Path,
        default=Path("data/external/groups"),
        help="Directory with place_{id}.json files",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/interim"),
        help="Output directory for CSV files",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level",
    )

    args = parser.parse_args()
    return Config(
        geo_csv_path=args.geo_csv,
        groups_dir=args.groups_dir,
        output_dir=args.output_dir,
        log_level=args.log_level,
    )


def validate_config(config: Config) -> None:
    """Validate input paths and configuration."""
    if not config.geo_csv_path.exists():
        raise FileNotFoundError(f"Geo CSV not found: {config.geo_csv_path}")
    if not config.groups_dir.exists():
        raise FileNotFoundError(f"Groups directory not found: {config.groups_dir}")
    if not config.groups_dir.is_dir():
        raise NotADirectoryError(f"Not a directory: {config.groups_dir}")

    config.output_dir.mkdir(parents=True, exist_ok=True)


def load_geo_data(geo_csv_path: Path) -> pd.DataFrame:
    """Load and validate geo data CSV."""
    geo_df = pd.read_csv(geo_csv_path)

    required_columns = {"number", "name", "lat", "lon", "link"}
    missing_columns = required_columns - set(geo_df.columns)
    if missing_columns:
        raise ValueError(f"Missing required columns: {sorted(missing_columns)}")

    geo_df = geo_df.rename(
        columns={
            "number": "place_id",
            "name": "place_name",
            "link": "vk_url",
        }
    )

    # light normalization
    geo_df["place_name"] = geo_df["place_name"].astype(str).str.strip()
    geo_df["vk_url"] = geo_df["vk_url"].astype(str).str.strip()

    return geo_df


def load_group_data(groups_dir: Path, place_id: int) -> Optional[BoardWithPlaceId]:
    """Load and validate group data using Pydantic model."""
    json_path = groups_dir / f"place_{place_id}.json"
    if not json_path.exists():
        logger.debug("Missing JSON file for place %s: %s", place_id, json_path)
        return None

    try:
        return BoardWithPlaceId.model_validate_json(json_path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning("Failed to parse JSON for place %s: %s", place_id, e)
        return None


def flatten_place_data(
    place_id: int,
    place_name: str,
    lat: float,
    lon: float,
    vk_url: str,
    board_with_place: Optional[BoardWithPlaceId],
) -> FlattenedData:
    """Flatten data for a single place using Pydantic models."""
    place_row: dict[str, Any] = {
        "place_id": place_id,
        "place_name": (place_name or "").strip(),
        "lat": lat,
        "lon": lon,
        "vk_url": (vk_url or "").strip(),
        "group_id": None,
        "group_name": None,
        "screen_name": None,
        "members_count": None,
        "description": None,
    }

    topics_data: list[dict[str, Any]] = []
    messages_data: list[dict[str, Any]] = []

    if board_with_place is None:
        return FlattenedData(place_row, topics_data, messages_data)

    group = board_with_place.board.group
    place_row.update(
        {
            "group_id": group.id,
            "group_name": group.name,
            "screen_name": group.screen_name,
            "members_count": group.members_count,
            "description": group.description,
        }
    )

    for topic in sorted(board_with_place.board.topics, key=lambda t: t.topic_id):
        topic_title = (topic.title or "").strip()
        topics_data.append(
            {
                "place_id": place_id,
                "group_id": group.id,
                "topic_id": topic.topic_id,
                "topic_title": topic_title,
                "message_count": len(topic.messages),
            }
        )

        for message_idx, message_text in enumerate(topic.messages):
            messages_data.append(
                {
                    "place_id": place_id,
                    "group_id": group.id,
                    "topic_id": topic.topic_id,
                    "message_idx": message_idx,
                    "message_text": message_text,
                }
            )

    return FlattenedData(place_row, topics_data, messages_data)


def process_all_places(
    geo_df: pd.DataFrame,
    groups_dir: Path,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, int]:
    """Process all places and return DataFrames with missing count."""
    all_place_data: list[dict[str, Any]] = []
    all_topic_data: list[dict[str, Any]] = []
    all_message_data: list[dict[str, Any]] = []
    missing_count = 0

    for row in geo_df.sort_values("place_id").itertuples(index=False):
        place_id = int(row.place_id)

        board_with_place = load_group_data(groups_dir, place_id)
        if board_with_place is None:
            missing_count += 1

        flattened = flatten_place_data(
            place_id=place_id,
            place_name=row.place_name,
            lat=row.lat,
            lon=row.lon,
            vk_url=row.vk_url,
            board_with_place=board_with_place,
        )

        all_place_data.append(flattened.place_data)
        all_topic_data.extend(flattened.topics_data)
        all_message_data.extend(flattened.messages_data)

    places_df = pd.DataFrame(all_place_data, columns=PLACES_COLUMNS).sort_values("place_id")

    topics_df = pd.DataFrame(all_topic_data, columns=TOPICS_COLUMNS)
    if not topics_df.empty:
        topics_df = topics_df.sort_values(["place_id", "topic_id"])

    messages_df = pd.DataFrame(all_message_data, columns=MESSAGES_COLUMNS)
    if not messages_df.empty:
        messages_df = messages_df.sort_values(["place_id", "topic_id", "message_idx"])

    return places_df, topics_df, messages_df, missing_count


def save_dataframes(
    places_df: pd.DataFrame,
    topics_df: pd.DataFrame,
    messages_df: pd.DataFrame,
    output_dir: Path,
) -> None:
    """Save DataFrames to CSV files."""
    output_dir.mkdir(parents=True, exist_ok=True)

    outputs: list[tuple[str, pd.DataFrame, Path]] = [
        ("places", places_df, output_dir / "places.csv"),
        ("topics", topics_df, output_dir / "topics.csv"),
        ("messages", messages_df, output_dir / "messages.csv"),
    ]

    for name, df, path in outputs:
        df.to_csv(path, index=False)
        logger.info("Saved %d rows to %s (%s)", len(df), path, name)


def main():
    """Main entry point for the flattening script."""
    try:
        config = setup_config()

        logging.basicConfig(
            level=getattr(logging, config.log_level),
            format="%(asctime)s - %(levelname)s - %(message)s",
        )

        validate_config(config)
        logger.info("Processing data from %s", config.groups_dir)

        geo_df = load_geo_data(config.geo_csv_path)
        logger.info("Loaded geo data for %d places", len(geo_df))

        places_df, topics_df, messages_df, missing_count = process_all_places(
            geo_df, config.groups_dir
        )

        save_dataframes(places_df, topics_df, messages_df, config.output_dir)

        logger.info("Processed %d places", len(places_df))
        logger.info("Extracted %d topics", len(topics_df))
        logger.info("Extracted %d messages", len(messages_df))

        if missing_count:
            logger.warning("Missing or invalid JSON files for %d places", missing_count)

    except Exception as e:
        logger.error("Failed to process data: %s", e, exc_info=True)


if __name__ == "__main__":
    main()
