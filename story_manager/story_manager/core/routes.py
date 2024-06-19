from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query

from commons.models.enums import Granularity
from commons.utilities.pagination import Page, PaginationParams
from story_manager.core.dependencies import CRUDStoryDep
from story_manager.core.enums import (
    Digest,
    Section,
    StoryGenre,
    StoryGroup,
    StoryType,
)
from story_manager.core.filters import StoryFilter
from story_manager.core.mappings import FILTER_MAPPING
from story_manager.core.models import Story

router = APIRouter(prefix="/stories", tags=["stories"])


def merge_lists(existing: list, new: list) -> list:
    return list(set(existing + new))


@router.get("/", response_model=Page[Story])
async def get_stories(
    story_crud: CRUDStoryDep,
    params: Annotated[PaginationParams, Depends(PaginationParams)],
    story_types: Annotated[list[StoryType], Query(description="List of story types")] = None,  # type: ignore
    story_groups: Annotated[list[StoryGroup], Query(description="List of story groups")] = None,  # type: ignore
    genres: Annotated[list[StoryGenre], Query(description="List of genres")] = None,  # type: ignore
    metric_ids: Annotated[list[str], Query(description="List of metric ids")] = None,  # type: ignore
    grains: Annotated[list[Granularity], Query(description="List of grains")] = None,  # type: ignore
    digest: Digest | None = None,
    section: Section | None = None,
    story_date_start: datetime | None = None,
    story_date_end: datetime | None = None,
) -> Any:
    """
    Retrieve stories.
    """
    predefined_filters = {
        "story_types": story_types,
        "story_groups": story_groups,
        "genres": genres,
    }

    # Apply predefined filters from FILTER_MAPPING if digest and section are provided
    if digest and section:
        mappings = FILTER_MAPPING.get((digest, section), {})
        for key, value in mappings.items():  # type: ignore
            if key in predefined_filters:
                predefined_filters[key] = (
                    merge_lists(predefined_filters[key], value) if isinstance(predefined_filters[key], list) else value  # type: ignore
                )
            else:
                predefined_filters[key] = value

    story_filter = StoryFilter(
        metric_ids=metric_ids,
        genres=predefined_filters.get("genres", None),  # type: ignore
        story_types=predefined_filters.get("story_types", None),  # type: ignore
        story_groups=predefined_filters.get("story_groups", None),  # type: ignore
        grains=grains,
        story_date_start=story_date_start,
        story_date_end=story_date_end,
        digest=None,
        section=None,
    )
    results, count = await story_crud.paginate(params=params, filter_params=story_filter.dict(exclude_unset=True))
    return Page.create(items=results, total_count=count, params=params)
