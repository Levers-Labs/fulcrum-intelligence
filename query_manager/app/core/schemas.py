import datetime

from app.utilities.schema import BaseModel


class Metric(BaseModel):
    id: int
    date: str
    name: str
    dimension: str
    slice: str
    value: int
