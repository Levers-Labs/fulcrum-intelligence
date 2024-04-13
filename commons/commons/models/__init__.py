from datetime import datetime

from pydantic import BaseModel as _BaseModel, ConfigDict

from commons.utilities.date_utils import convert_datetime_to_utc


class BaseModel(_BaseModel):
    """
    Custom base class for pydantic models
    """

    model_config = ConfigDict(json_encoders={datetime: convert_datetime_to_utc}, populate_by_name=True)
