import datetime
from typing import List, Optional

from app.utilities.schema import BaseModel


class Metric(BaseModel):
    id: int
    date: datetime.date
    name: str
    dimension: str
    slice: str
    value: int


class DimensionFilter(BaseModel):
    dimension: str
    slices: Optional[List[str]] = None
