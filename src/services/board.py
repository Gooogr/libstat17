from src.clients.vk import VKClient  # type: ignore[import-untyped]
from src.dto.dto_group import Board, Group, Topic

DEFAULT_GROUP_FIELDS = "screen_name,name,is_closed,description,members_count"


class VKBoardService:
    def __init__(self, client: VKClient, fields: str = DEFAULT_GROUP_FIELDS):
        self.client = client
        self.fields = fields

    def get_group(self, group_input: str) -> Group | None:
        slug = self.client.slug(group_input)
        if not slug:
            return None
        items = (
            self.client.call(
                self.client.api.groups.getById, group_ids=slug, fields=self.fields
            )
            or []
        )
        return Group.model_validate(items[0]) if items else None

    def get_topic_messages(self, group_id: int, topic_id: int) -> list[str]:
        out: list[str] = []
        for c in self.client.paginate(
            self.client.api.board.getComments, group_id=group_id, topic_id=topic_id
        ):
            text = (c.get("text") or "").strip()
            if text:
                out.append(text)
        return out

    def dump_board(self, group: Group) -> Board:
        topics: list[Topic] = []
        for t in self.client.paginate(
            self.client.api.board.getTopics, group_id=group.id
        ):
            topic_id = int(t["id"])
            title = (t.get("title") or "").strip()
            topics.append(
                Topic(
                    topic_id=topic_id,
                    title=title,
                    messages=self.get_topic_messages(group.id, topic_id),
                )
            )
        return Board(group=group, topics=topics)
