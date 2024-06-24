from pydantic import EmailStr
from sqlmodel import AutoString, Field

from analysis_manager.core.models.base_model import AnalysisSchemaBaseModel


class UserBase(AnalysisSchemaBaseModel):
    first_name: str = Field(max_length=255)
    last_name: str = Field(max_length=255)
    email: EmailStr = Field(max_length=255, unique=True, sa_type=AutoString, index=True)
    is_active: bool = Field(default=True)


class User(UserBase, table=True):  # type: ignore
    password: str = Field(max_length=255)


class UserRead(UserBase):
    id: int
