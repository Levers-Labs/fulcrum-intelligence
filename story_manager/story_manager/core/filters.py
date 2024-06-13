from datetime import datetime

from commons.db.filters import BaseFilter, FilterField
from commons.models.enums import Granularity
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.core.models import Story


class StoryFilter(BaseFilter[Story]):
    metric_ids: list[str] | None = FilterField(Story.metric_id, operator="in", default=None)  # type: ignore
    story_date_start: datetime | None = FilterField(Story.story_date, operator="ge", default=None)  # type: ignore
    story_date_end: datetime | None = FilterField(Story.story_date, operator="le", default=None)  # type: ignore
    genres: list[StoryGenre] | None = FilterField(Story.genre, operator="in", default=None)  # type: ignore
    story_types: list[StoryType] | None = FilterField(Story.story_type, operator="in", default=None)  # type: ignore
    story_groups: list[StoryGroup] | None = FilterField(Story.story_group, operator="in", default=None)  # type: ignore
    grains: list[Granularity] | None = FilterField(Story.grain, operator="in", default=None)  # type: ignore
