from datetime import datetime

from commons.db.filters import BaseFilter, FilterField
from commons.models.enums import Granularity
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.core.models import Story


class StoryFilter(BaseFilter[Story]):
    metric_id: str | None = FilterField(Story.metric_id, operator="eq", default=None)  # type: ignore
    created_at_start: datetime | None = FilterField(Story.created_at, operator="ge", default=None)  # type: ignore
    created_at_end: datetime | None = FilterField(Story.created_at, operator="le", default=None)  # type: ignore
    genre: StoryGenre | None = FilterField(Story.genre, operator="eq", default=None)  # type: ignore
    story_type: StoryType | None = FilterField(Story.story_type, operator="eq", default=None)  # type: ignore
    story_group: StoryGroup | None = FilterField(Story.story_group, operator="eq", default=None)  # type: ignore
    grain: Granularity | None = FilterField(Story.grain, operator="eq", default=None)  # type: ignore
