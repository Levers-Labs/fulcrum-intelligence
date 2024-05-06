from __future__ import annotations

from typing import Annotated

from fastapi import Depends

from story_manager.core.crud import CRUDStory
from story_manager.core.models import Story
from story_manager.db.config import AsyncSessionDep


async def get_stories_crud(session: AsyncSessionDep) -> CRUDStory:
    return CRUDStory(model=Story, session=session)


CRUDStoryDep = Annotated[CRUDStory, Depends(get_stories_crud)]
