from datetime import datetime

from commons.db.filters import BaseFilter, FilterField
from commons.models.enums import Granularity
from story_manager.core.enums import (
    Digest,
    Section,
    StoryGenre,
    StoryGroup,
    StoryType,
)
from story_manager.core.mappings import FILTER_MAPPING
from story_manager.core.models import Story


class StoryFilter(BaseFilter[Story]):
    metric_ids: list[str] | None = FilterField(Story.metric_id, operator="in", default=None)  # noqa
    story_date_start: datetime | None = FilterField(Story.story_date, operator="ge", default=None)  # noqa
    story_date_end: datetime | None = FilterField(Story.story_date, operator="le", default=None)  # noqa
    genres: list[StoryGenre] | None = FilterField(Story.genre, operator="in", default=None)  # noqa
    story_types: list[StoryType] | None = FilterField(Story.story_type, operator="in", default=None)  # noqa
    story_groups: list[StoryGroup] | None = FilterField(Story.story_group, operator="in", default=None)  # noqa
    grains: list[Granularity] | None = FilterField(Story.grain, operator="in", default=None)  # noqa
    digest: Digest | None  # noqa
    section: Section | None  # noqa

    def apply_predefined_filters(self) -> None:
        if self.digest and self.section:
            mappings = FILTER_MAPPING.get((self.digest, self.section), {})
            for key, value in mappings.items():  # type: ignore
                current_value = getattr(self, key)
                if isinstance(current_value, list):
                    setattr(self, key, list(set(current_value + value)))
                else:
                    setattr(self, key, value)
