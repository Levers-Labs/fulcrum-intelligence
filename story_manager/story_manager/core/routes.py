from datetime import datetime
from typing import Annotated, Any

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
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
from story_manager.core.schemas import StoryDetail, StoryGroupMeta
from story_manager.story_builder import StoryBuilderBase
from story_manager.story_builder.factory import StoryFactory

router = APIRouter(prefix="/stories", tags=["stories"])


@router.get(
    "/groups/{group}",
    response_model=StoryGroupMeta,
    dependencies=[Security(oauth2_auth().verify, scopes=[STORY_MANAGER_ALL])],
)
async def get_story_group_meta(group: StoryGroup) -> StoryGroupMeta:
    builder_klass: type[StoryBuilderBase] = StoryFactory.get_story_builder(group)
    if builder_klass is None:
        raise HTTPException(status_code=404)
    return StoryGroupMeta(group=group, grains=builder_klass.supported_grains)


@router.get(
    "/", response_model=Page[StoryDetail],
    # dependencies=[Security(oauth2_auth().verify, scopes=[STORY_MANAGER_ALL])]
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
    Retrieve stories.
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
    )

    results, count = await story_crud.paginate(params=params, filter_params=story_filter.model_dump(exclude_unset=True))
    return Page.create(items=results, total_count=count, params=params)
