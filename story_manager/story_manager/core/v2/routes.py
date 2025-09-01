import logging
import time
from datetime import datetime
from typing import Annotated, Any

from fastapi import (
    APIRouter,
    Depends,
    Query,
    Security,
)

from commons.auth.scopes import STORY_MANAGER_ALL
from commons.models.enums import Granularity
from commons.utilities.pagination import Page, PaginationParams
from story_manager.core.dependencies import CRUDStoryDep, oauth2_auth
from story_manager.core.enums import (
    Digest,
    Section,
    StoryGenre,
    StoryGroup,
    StoryType,
)
from story_manager.core.filters import StoryFilter
from story_manager.core.schemas import StoryDetail, StoryStatsResponse

router = APIRouter(prefix="/stories", tags=["stories"])
logger = logging.getLogger(__name__)


@router.get(
    "/", response_model=Page[StoryDetail], dependencies=[Security(oauth2_auth().verify, scopes=[STORY_MANAGER_ALL])]
)
async def get_stories(
    story_crud: CRUDStoryDep,
    params: Annotated[PaginationParams, Depends(PaginationParams)],
    story_types: Annotated[list[StoryType], Query(description="List of story types")] = None,  # type: ignore
    story_groups: Annotated[list[StoryGroup], Query(description="List of story groups")] = None,  # type: ignore
    genres: Annotated[list[StoryGenre], Query(description="List of genres")] = None,  # type: ignore
    metric_ids: Annotated[list[str], Query(description="List of metric ids")] = None,  # type: ignore
    grains: Annotated[list[Granularity], Query(description="List of grains")] = None,  # type: ignore
    story_date_start: datetime | None = None,
    story_date_end: datetime | None = None,
    digest: Digest | None = None,
    section: Section | None = None,
    is_heuristic: bool | None = None,
) -> Any:
    """
    Retrieve stories with detailed performance profiling.
    """
    t_start = time.perf_counter()

    # Step 1: Build filter
    t1 = time.perf_counter()
    story_filter = StoryFilter(
        metric_ids=metric_ids,
        genres=genres,
        story_types=story_types,
        story_groups=story_groups,
        grains=grains,
        story_date_start=story_date_start,
        story_date_end=story_date_end,
        digest=digest,
        section=section,
        is_heuristic=is_heuristic,
        version=2,
    )
    t2 = time.perf_counter()
    filter_dict = story_filter.model_dump(exclude_unset=True)
    t3 = time.perf_counter()

    # Step 2: Database call (paginate method)
    results, count = await story_crud.paginate(params=params, filter_params=filter_dict)
    t4 = time.perf_counter()

    # Step 3: Create page response
    page_result = Page.create(items=results, total_count=count, params=params)
    t5 = time.perf_counter()

    # Log detailed timing
    logger.warning(
        "PERF: v2/stories limit=%d | "
        "filter_build=%.2fms | filter_dump=%.2fms | db_paginate=%.2fms | page_create=%.2fms | "
        "TOTAL=%.2fms | rows_returned=%d",
        params.limit,
        (t2 - t1) * 1000,
        (t3 - t2) * 1000,
        (t4 - t3) * 1000,
        (t5 - t4) * 1000,
        (t5 - t_start) * 1000,
        len(results) if results else 0,
    )

    return page_result


@router.get(
    "/stats",
    response_model=list[StoryStatsResponse],
    dependencies=[Security(oauth2_auth().verify, scopes=[STORY_MANAGER_ALL])],
)
async def get_story_stats(
    story_crud: CRUDStoryDep,
    story_types: Annotated[list[StoryType], Query(description="List of story types")] = None,  # type: ignore
    story_groups: Annotated[list[StoryGroup], Query(description="List of story groups")] = None,  # type: ignore
    genres: Annotated[list[StoryGenre], Query(description="List of genres")] = None,  # type: ignore
    metric_ids: Annotated[list[str], Query(description="List of metric ids")] = None,  # type: ignore
    grains: Annotated[list[Granularity], Query(description="List of grains")] = None,  # type: ignore
    story_date_start: datetime | None = None,
    story_date_end: datetime | None = None,
    digest: Digest | None = None,
    section: Section | None = None,
    is_heuristic: bool | None = None,
) -> list[StoryStatsResponse]:
    """
    Get simple story stats count grouped by date.

    Returns a list of dates with the count of stories for each date that match the provided filters.
    """
    story_filter = StoryFilter(
        metric_ids=metric_ids,
        genres=genres,
        story_types=story_types,
        story_groups=story_groups,
        grains=grains,
        story_date_start=story_date_start,
        story_date_end=story_date_end,
        digest=digest,
        section=section,
        is_heuristic=is_heuristic,
        version=2,
    )

    # Get the stats and return
    return await story_crud.get_story_stats(filter_params=story_filter.model_dump(exclude_unset=True))
