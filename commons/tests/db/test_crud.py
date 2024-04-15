from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import BaseModel
from sqlmodel.ext.asyncio.session import AsyncSession

from commons.db.crud import CRUDBase, NotFoundError
from commons.db.models import BaseDBModel


# Assume we have some hypothetical models and schemas
class ExampleModel(BaseDBModel, table=True):
    name: str


class ExampleCreateSchema(BaseModel):
    name: str


class ExampleUpdateSchema(BaseModel):
    name: str


@pytest.fixture
def async_session():
    # Create a complete mock of the AsyncSession
    session = MagicMock(spec=AsyncSession)
    # Setting AsyncMock for all async methods
    session.execute = AsyncMock()
    session.commit = AsyncMock()
    session.refresh = AsyncMock()
    session.delete = AsyncMock()
    return session


@pytest.fixture
def crud_base(async_session):
    return CRUDBase(ExampleModel, async_session)


@pytest.mark.asyncio
async def test_get_found(crud_base, async_session):
    # Setup the expected return values for execute
    mock_result = MagicMock()
    expected_instance = ExampleModel(id=1, name="Test Item")
    mock_result.scalar_one_or_none.return_value = expected_instance
    async_session.execute.return_value = mock_result

    # Act
    result = await crud_base.get(id=1)

    # Assert
    assert result == expected_instance
    async_session.execute.assert_awaited()


@pytest.mark.asyncio
async def test_get_not_found(crud_base, async_session):
    # Setup execute to return a MagicMock with None for scalar_one_or_none
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None
    async_session.execute.return_value = mock_result

    # Act & Assert
    with pytest.raises(NotFoundError):
        await crud_base.get(id=1)


@pytest.mark.asyncio
async def test_list(crud_base, async_session):
    # Set up the expected model instances to return
    model_instances = [ExampleModel(id=1, name="Item One"), ExampleModel(id=2, name="Item Two")]

    # Set up to execute mock to properly chain calls to scalars().all()
    execute_result = MagicMock()  # Mock the result of session.execute
    scalars_result = MagicMock()  # Mock the result of result.scalars()
    scalars_result.all.return_value = model_instances
    execute_result.scalars.return_value = scalars_result
    async_session.execute.return_value = execute_result

    # Act
    results = await crud_base.list()

    # Assert
    assert results == model_instances
    async_session.execute.assert_awaited()


@pytest.mark.asyncio
async def test_create(crud_base, async_session):
    obj_in = ExampleCreateSchema(name="New Item")
    new_instance = ExampleModel(id=None, name="New Item")

    # Setting up the mock to handle the commit and refresh patterns
    async_session.execute.return_value.scalars.return_value.all.return_value = [new_instance]

    # Act
    result = await crud_base.create(obj_in=obj_in)

    # Assert
    assert result == new_instance
    async_session.add.assert_called_with(new_instance)
    async_session.commit.assert_awaited()
    async_session.refresh.assert_awaited_with(new_instance)


@pytest.mark.asyncio
async def test_update(crud_base, async_session):
    original_obj = ExampleModel(id=1, name="Old Item")
    obj_in = ExampleModel(id=1, name="Updated Item")

    # Act
    result = await crud_base.update(obj=original_obj, obj_in=obj_in)

    # Assert
    assert result.name == "Updated Item"
    async_session.add.assert_called_with(original_obj)
    async_session.commit.assert_awaited()
    async_session.refresh.assert_awaited_with(original_obj)


@pytest.mark.asyncio
async def test_delete(crud_base, async_session):
    # Mock instance to be deleted
    instance_to_delete = ExampleModel(id=1, name="Item to Delete")

    # Setup the expected return values for executing
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = instance_to_delete
    async_session.execute.return_value = mock_result

    # Act
    await crud_base.delete(id=1)

    # Assert
    async_session.delete.assert_called_once_with(instance_to_delete)
    async_session.commit.assert_awaited()


@pytest.mark.asyncio
async def test_total_count(crud_base, async_session):
    mock_result = MagicMock()
    mock_result.one.return_value = (10,)  # Simulating the return of a single tuple with one element
    async_session.execute.return_value = mock_result  # make sure this is awaitable and returns mock_result

    # Act
    result = await crud_base.total_count()

    # Assert
    assert result == 10
