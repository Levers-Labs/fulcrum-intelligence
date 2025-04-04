from typing import (
    Any,
    Generic,
    TypeVar,
    cast,
)

from fastapi import HTTPException
from pydantic import BaseModel
from sqlalchemy import Select, func, select
from sqlmodel.ext.asyncio.session import AsyncSession

from commons.db.filters import BaseFilter
from commons.db.models import BaseSQLModel
from commons.utilities.pagination import PaginationParams

ModelType = TypeVar("ModelType", bound=BaseSQLModel)
FilterType = TypeVar("FilterType", bound=BaseFilter)
CreateSchemaType = TypeVar("CreateSchemaType", bound=BaseModel)
UpdateSchemaType = TypeVar("UpdateSchemaType", bound=BaseModel)


class NotFoundError(HTTPException):
    def __init__(self, id: Any) -> None:
        self.id = id
        super().__init__(404, f"Object with id {id} not found")


class CRUDBase(Generic[ModelType, CreateSchemaType, UpdateSchemaType, FilterType]):
    filter_class: type[FilterType] | None = None

    def __init__(self, model: type[ModelType], session: AsyncSession):
        """
        CRUD object with default methods to Create, Read, Update, Delete.

        **Parameters**

        * `Model`: A SQLAlchemy model class
        * `session`: A SQLAlchemy async session
        """
        self.model = model
        self.session = session

    def get_select_query(self) -> Select:
        return select(self.model)

    async def get(self, id: int) -> ModelType:
        statement = self.get_select_query().filter_by(id=id)
        results = await self.session.execute(statement=statement)
        instance: ModelType | None = results.unique().scalar_one_or_none()

        if instance is None:
            raise NotFoundError(id=id)

        return instance

    async def list_results(self, *, params: PaginationParams) -> list[ModelType]:
        results = await self.session.execute(self.get_select_query().offset(params.offset).limit(params.limit))
        records: list[ModelType] = cast(list[ModelType], results.scalars().all())
        return records

    async def paginate(self, params: PaginationParams, filter_params: dict[str, Any]) -> tuple[list[ModelType], int]:
        """
        Retrieve a paginated list of results.
        Apply filters if provided using the filter class.
        Returns a tuple of the results and the total count.
        """
        # base query
        query = self.get_select_query()
        # apply filters if filter class is provided
        if self.filter_class is not None:
            query = self.filter_class.apply_filters(query, filter_params)

        # get total count
        count_query = select(func.count()).select_from(query.subquery())
        count = await self.session.scalar(count_query)

        # get paginated results
        results_query = query.offset(params.offset).limit(params.limit)
        results = await self.session.scalars(results_query)
        records: list[ModelType] = cast(list[ModelType], results.unique().all())
        return records, count or 0

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
