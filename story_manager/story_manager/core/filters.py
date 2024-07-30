from datetime import datetime
from typing import Any

from sqlalchemy import Select

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
    metric_ids: list[str] | None = FilterField(Story.metric_id, operator="in", default=None)  # type: ignore
    story_date_start: datetime | None = FilterField(Story.story_date, operator="ge", default=None)  # type: ignore
    story_date_end: datetime | None = FilterField(Story.story_date, operator="le", default=None)  # type: ignore
    genres: list[StoryGenre] | None = FilterField(Story.genre, operator="in", default=None)  # type: ignore
    story_types: list[StoryType] | None = FilterField(Story.story_type, operator="in", default=None)  # type: ignore
    story_groups: list[StoryGroup] | None = FilterField(Story.story_group, operator="in", default=None)  # type: ignore
    grains: list[Granularity] | None = FilterField(Story.grain, operator="in", default=None)  # type: ignore
    digest: Digest | None = FilterField(None, default=None)  # type: ignore
    section: Section | None = FilterField(None, default=None)  # type: ignore

    @classmethod
    def apply_filters(cls, query: Select, values: dict[str, Any]) -> Select:
        digest = values.pop("digest", None)
        section = values.pop("section", None)

        if digest and section:
            mapping = FILTER_MAPPING.get((digest, section))
            if mapping:
                # logger.debug("Applying mapping for (%s, %s): %s", digest, section, mapping)
                for key, value in mapping.items():  # type: ignore
                    if key in values and values[key] is not None:
                        # Merge lists if the key already exists in values
                        if isinstance(values[key], list):
                            values[key] = list(set(values[key] + value))
                        else:
                            values[key] = value
                    else:
                        values[key] = value

        return super().apply_filters(query, values)
