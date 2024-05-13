from typing import Annotated, Any

from fastapi import APIRouter, Depends

from commons.utilities.pagination import Page, PaginationParams
from story_manager.core.dependencies import CRUDStoryDep
from story_manager.core.filters import StoryFilter
from story_manager.core.models import Story

router = APIRouter(prefix="/stories", tags=["stories"])


@router.get("/", response_model=Page[Story])
async def get_stories(
    story_crud: CRUDStoryDep,
    params: Annotated[PaginationParams, Depends(PaginationParams)],
    filters: Annotated[StoryFilter, Depends(StoryFilter)],
) -> Any:
    """
    Retrieve stories.
    """

    results, count = await story_crud.paginate(params=params, filter_params=filters.dict(exclude_unset=True))
    return Page.create(items=results, total_count=count, params=params)
