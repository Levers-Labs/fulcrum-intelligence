from typing import (
    Any,
    Generic,
    TypeVar,
    cast,
)

from fastapi import HTTPException
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlmodel.ext.asyncio.session import AsyncSession
from starlette.status import HTTP_404_NOT_FOUND

from app.db.models import ModelBase

ModelType = TypeVar("ModelType", bound=ModelBase)
CreateSchemaType = TypeVar("CreateSchemaType", bound=BaseModel)
UpdateSchemaType = TypeVar("UpdateSchemaType", bound=BaseModel)


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
            raise HTTPException(status_code=HTTP_404_NOT_FOUND)

        return instance

    async def list(self, *, offset: int = 0, limit: int = 100) -> list[ModelType]:
        results = await self.session.execute(select(self.model).offset(offset).limit(limit))
        records: list[ModelType] = cast(list[ModelType], results.scalars().all())
        return records

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
