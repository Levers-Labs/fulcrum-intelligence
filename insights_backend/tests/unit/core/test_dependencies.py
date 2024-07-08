from unittest.mock import AsyncMock

import pytest

from insights_backend.core.dependencies import get_users_crud
from insights_backend.core.models.users import User


@pytest.mark.asyncio
async def test_get_stories_crud():
    mock_db_session = AsyncMock()
    crud = await get_users_crud(session=mock_db_session)
    assert crud.model == User
    assert crud.session == mock_db_session
