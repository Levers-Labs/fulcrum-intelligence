from unittest.mock import AsyncMock

import pytest

from commons.clients.analysis_manager import AnalysisManagerClient
from commons.clients.query_manager import QueryManagerClient
from fulcrum_core import AnalysisManager
from story_manager.core.dependencies import (
    get_analysis_manager,
    get_analysis_manager_client,
    get_query_manager_client,
    get_stories_crud,
)
from story_manager.core.models import Story


@pytest.mark.asyncio
async def test_get_stories_crud():
    mock_db_session = AsyncMock()
    crud = await get_stories_crud(session=mock_db_session)
    assert crud.model == Story
    assert crud.session == mock_db_session


@pytest.mark.asyncio
async def test_get_query_manager_client():
    client = await get_query_manager_client()
    assert isinstance(client, QueryManagerClient)


@pytest.mark.asyncio
async def test_get_analysis_manager_client():
    client = await get_analysis_manager_client()
    assert isinstance(client, AnalysisManagerClient)


@pytest.mark.asyncio
async def test_get_analysis_manager():
    client = await get_analysis_manager()
    assert isinstance(client, AnalysisManager)
