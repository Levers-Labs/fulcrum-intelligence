from typing import (
    Any,
    Generic,
    TypeVar,
    cast,
)

from pydantic import BaseModel
from sqlalchemy import func, select
from sqlmodel.ext.asyncio.session import AsyncSession

from commons.db.models import BaseSQLModel
from commons.utilities.pagination import PaginationParams

ModelType = TypeVar("ModelType", bound=BaseSQLModel)
CreateSchemaType = TypeVar("CreateSchemaType", bound=BaseModel)
UpdateSchemaType = TypeVar("UpdateSchemaType", bound=BaseModel)


class NotFoundError(Exception):
    def __init__(self, id: Any) -> None:
        self.id = id
        super().__init__(f"Object with id {id} not found")


class CRUDBase(Generic[ModelType, CreateSchemaType, UpdateSchemaType]):
    def __init__(self, model: type[ModelType], session: AsyncSession):
        """
        CRUD object with default methods to Create, Read, Update, Delete.

        **Parameters**

        * `Model`: A SQLAlchemy model class
        * `session`: A SQLAlchemy async session
        """
        self.model = model
        self.session = session

    async def get(self, id: int) -> ModelType:
        statement = select(self.model).filter_by(id=id)
        results = await self.session.execute(statement=statement)
        instance: ModelType | None = results.scalar_one_or_none()

        if instance is None:
            raise NotFoundError(id=id)

        return instance

    async def list_results(self, *, params: PaginationParams) -> list[ModelType]:
        results = await self.session.execute(select(self.model).offset(params.offset).limit(params.limit))
        records: list[ModelType] = cast(list[ModelType], results.scalars().all())
        return records

    async def list_with_count(self, *, params: PaginationParams) -> tuple[list[ModelType], int]:
        """
        Return a tuple of the list of records and the total count of records
        """
        records = await self.list_results(params=params)
        total_count = await self.total_count()
        return records, total_count

    async def create(self, *, obj_in: CreateSchemaType) -> ModelType:
        values = obj_in.dict()
        obj = self.model(**values)
        self.session.add(obj)
        await self.session.commit()
        await self.session.refresh(obj)
        return obj

    async def update(self, *, obj: ModelType, obj_in: UpdateSchemaType | dict[str, Any]) -> ModelType:
        obj_data = obj.dict()
        if isinstance(obj_in, dict):
            update_data = obj_in
        else:
            update_data = obj_in.dict(exclude_unset=True)
        for field in obj_data:
            if field in update_data:
                setattr(obj, field, update_data[field])

        self.session.add(obj)
        await self.session.commit()
        await self.session.refresh(obj)
        return obj

    async def delete(self, *, id: Any) -> None:
        instance = await self.get(id=id)
        await self.session.delete(instance)
        await self.session.commit()

    async def total_count(self) -> int:
        result = await self.session.execute(select(func.count()).select_from(self.model))
        return result.one()[0]  # type: ignore
