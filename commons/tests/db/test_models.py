from datetime import datetime

from commons.db.models import (
    BaseDBModel,
    BaseSQLModel,
    BaseTimeStampedModel,
    convert_datetime_to_utc,
)
from commons.models import BaseModel


def test_base_model_json_encoder():
    """Test that datetime objects are correctly converted to UTC JSON format."""
    example_datetime = datetime(2023, 1, 1, 12, 0)
    json_output = BaseModel.model_config["json_encoders"][datetime](example_datetime)
    expected = convert_datetime_to_utc(example_datetime)
    assert json_output == expected, "JSON encoder for datetime should convert to UTC"


def test_base_sql_model_json_encoder():
    """Test that datetime objects are correctly converted to UTC JSON format."""
    example_datetime = datetime(2023, 1, 1, 12, 0)
    json_output = BaseSQLModel.model_config["json_encoders"][datetime](example_datetime)
    expected = convert_datetime_to_utc(example_datetime)
    assert json_output == expected, "JSON encoder for datetime should convert to UTC"


def test_base_db_model_defaults():
    """Test default ID initialization in BaseDBModel."""
    instance = BaseDBModel()
    assert instance.id is None, "Default id should be None"


def test_base_timestamped_model_defaults():
    """Test default timestamps in BaseTimeStampedModel."""
    instance = BaseTimeStampedModel()
    assert instance.created_at is not None, "created_at should have a default value"
    assert instance.updated_at is not None, "updated_at should have a default value"

    # Check if timestamps are reasonably close to "now"
    now = datetime.utcnow()
    assert abs((now - instance.created_at).total_seconds()) < 5, "created_at should be close to current UTC time"
    assert abs((now - instance.updated_at).total_seconds()) < 5, "updated_at should be close to current UTC time"
