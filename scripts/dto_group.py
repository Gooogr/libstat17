from pydantic import BaseModel, ConfigDict, Field


class Group(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: int
    name: str | None = None
    screen_name: str | None = None
    members_count: int | None = None
    is_closed: int | None = None
    description: str | None = None


class Topic(BaseModel):
    topic_id: int
    title: str = ""
    messages: list[str] = Field(default_factory=list)


class Board(BaseModel):
    group: Group
    topics: list[Topic] = Field(default_factory=list)
