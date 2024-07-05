from sqlalchemy import Column, Integer, String
from sqlmodel import Field

from commons.db.models import BaseTimeStampedModel


class InsightsSchemaBaseModel(BaseTimeStampedModel):
    __table_args__ = {"schema": "insights_store"}


class Users(InsightsSchemaBaseModel):
    id: int = Field(sa_column=Column(Integer, primary_key=True, index=True))
    name: str = Field(sa_column=Column(String, unique=True, index=True, nullable=False))
    provider: str = Field(sa_column=Column(String, nullable=False))
    external_user_id: str = Field(sa_column=Column(String, unique=True, nullable=False))
    profile_pic: str = Field(sa_column=Column(String, nullable=True))
