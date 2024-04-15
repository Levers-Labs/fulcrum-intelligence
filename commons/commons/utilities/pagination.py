from __future__ import annotations

from typing import Generic, TypeVar

from pydantic import BaseModel, Field
from pydantic.generics import GenericModel

T = TypeVar("T", bound=BaseModel)


class Params(BaseModel):
    limit: int = Field(10, gt=0)
    offset: int = Field(0, gt=-1)


class Page(GenericModel, Generic[T]):
    results: list[T]
    count: int
