from typing import Literal
from pydantic import BaseModel, ConfigDict, Field

# -----------------------------
# Shared input payload (per topic)
# -----------------------------
class TopicTextPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    place_id: int
    group_id: int
    topic_id: int
    topic_title: str = ""
    topic_text: str  # all concatenated messages in topic


# -----------------------------
# Book wishes: response schema
# -----------------------------
class BookWishItemRow(BaseModel):
    model_config = ConfigDict(extra="forbid")

    place_id: int
    group_id: int
    topic_id: int

    author: str = Field(default="")
    book_title: str

    confidence: float = Field(..., ge=0.0, le=1.0)


class BookWishExtractionResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    rows: list[BookWishItemRow]


# -----------------------------
# Nonbook wishes: response schema
# -----------------------------
NonbookCategory = Literal[
    "furniture",
    "tech_equipment",
    "supplies",
    "nonbook_activities",
    "facility_care",
    "event_decor",
    "other",
]


class NonbookWishItemRow(BaseModel):
    model_config = ConfigDict(extra="forbid")

    place_id: int
    group_id: int
    topic_id: int

    object_name: str
    category: NonbookCategory

    # empty string if no/unclear url; keep URLs as-is
    object_url: str = Field(default="")

    confidence: float = Field(..., ge=0.0, le=1.0)


class NonbookWishExtractionResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    rows: list[NonbookWishItemRow]
