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
    created_at_start: datetime | None = None,
    created_at_end: datetime | None = None,
    digest: Digest | None = None,
    section: Section | None = None,
) -> Any:
    """
    Retrieve stories.
    """
    predefined_filters = {
        "story_types": story_types if story_types else [],
        "story_groups": story_groups if story_groups else [],
        "genres": genres if genres else [],
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
        genres=predefined_filters.get("genres"),
        story_types=predefined_filters.get("story_types"),
        story_groups=predefined_filters.get("story_groups"),
        grains=grains,
        created_at_start=created_at_start,
        created_at_end=created_at_end,
        digest=digest,
        section=section,
    )
    results, count = await story_crud.paginate(params=params, filter_params=story_filter.dict(exclude_unset=True))
    return Page.create(items=results, total_count=count, params=params)
