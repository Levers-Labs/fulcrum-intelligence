from typing import Generic, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T", bound=BaseModel)


class PaginationParams(BaseModel):
    limit: int = Field(10, gt=0, description="Maximum number of items to return")
    offset: int = Field(0, ge=0, description="Number of items to skip before returning the results")


class Page(BaseModel, Generic[T]):
    count: int
    limit: int
    offset: int
    pages: int
    results: list[T]

    @classmethod
    def create(cls, items: list[T], total_count: int, params: PaginationParams):
        total_pages = (total_count + params.limit - 1) // params.limit
        return cls(
            results=items,
            count=total_count,
            limit=params.limit,
            offset=params.offset,
            pages=total_pages,
        )
