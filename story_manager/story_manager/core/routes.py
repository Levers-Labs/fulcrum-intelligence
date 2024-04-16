from typing import Annotated, Any

from fastapi import APIRouter, Depends

from commons.utilities.pagination import Page, PaginationParams
from story_manager.core.dependencies import CRUDStoryDep
from story_manager.core.enums import GENRE_TO_STORY_TYPE_MAPPING, STORY_TYPES_META
from story_manager.core.filters import StoryFilter
from story_manager.core.models import Story
from story_manager.core.schemas import StoryGenreSchema

router = APIRouter(prefix="/stories", tags=["stories"])


@router.get("/genres", response_model=list[StoryGenreSchema])
def get_story_genres() -> Any:
    """
    Retrieve story genres.
    """
    genres = []
    for genre, types in GENRE_TO_STORY_TYPE_MAPPING.items():
        genre_obj = {"genre": genre, "types": [], "label": genre.name.replace("_", " ").title()}  # type: ignore
        # for each type, populate the label, desc.
        for story_type in types:
            type_meta = STORY_TYPES_META[story_type]
            genre_obj["types"].append(  # type: ignore
                {
                    "type": story_type,
                    "label": type_meta["label"],
                    "description": type_meta["description"],
                }
            )
        genres.append(genre_obj)
    return genres


# todo: add metric id filter, created_at range , genre, story type filters
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
