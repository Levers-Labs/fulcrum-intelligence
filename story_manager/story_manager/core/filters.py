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
    is_salient: bool | None = FilterField(Story.is_salient, operator="eq", default=None)  # type: ignore

    @classmethod
    def apply_filters(cls, query: Select, values: dict[str, Any]) -> Select:
        """
        Apply filters to the query based on the digest and section, using the FILTER_MAPPING.

        This method enhances the filter values by adding or merging additional filters
        defined in the FILTER_MAPPING for the given digest and section combination.

        Args:
            query (Select): The initial SQL query.
            values (dict[str, Any]): A dictionary of filter values, including 'digest' and 'section'.

        Returns:
            Select: The modified SQL query with applied filters.

        Example:
            Initial values:
            {
                'digest': Digest.PORTFOLIO,
                'section': Section.PROMISING_TRENDS,
                'other_filter': 'some_value'
            }

            After apply_filters:
            {
                'other_filter': 'some_value',
                'story_types': [
                    StoryType.IMPROVING_PERFORMANCE,
                    StoryType.ACCELERATING_GROWTH,
                    StoryType.NEW_UPWARD_TREND
                ]
            }

            The method adds 'story_types' based on the FILTER_MAPPING for PORTFOLIO digest
            and PROMISING_TRENDS section, while keeping the original 'other_filter'.
        """
        digest = values.pop("digest", None)
        section = values.pop("section", None)

        if digest and section:
            # Look up the mapping for the given digest and section combination
            mapping = FILTER_MAPPING.get((digest, section))
            if mapping:
                # Iterate through the mapping items
                for key, value in mapping.items():  # type: ignore
                    if key in values and values[key] is not None:
                        # If the key already exists in values, merge the lists
                        if isinstance(values[key], list):
                            # Use set to remove duplicates when merging
                            values[key] = list(set(values[key] + value))
                        else:
                            # If it's not a list, simply replace the value
                            values[key] = value
                    else:
                        # If the key doesn't exist in values, add it
                        values[key] = value

            # Call the parent class's apply_filters method with the updated values
        return super().apply_filters(query, values)
