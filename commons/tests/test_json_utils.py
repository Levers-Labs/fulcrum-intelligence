from datetime import date, datetime, time
from decimal import Decimal
from uuid import UUID

from pytz import utc

from commons.utilities.json_utils import serialize_json


def test_serialize_json():
    # Prepare
    sample_data = {
        "date": datetime(2021, 1, 1, 0, 0, 0, 123456, tzinfo=utc),
        "decimal": Decimal("10.5"),
        "date_only": date(2021, 1, 1),
        "uuid": UUID("123e4567-e89b-12d3-a456-426614174000"),
        "time": time(12, 34, 56, 123456, tzinfo=utc),
    }

    # Execute
    result = serialize_json(sample_data)

    # Assert
    assert result == {
        "date": "2021-01-01T00:00:00.123Z",
        "decimal": "10.5",
        "date_only": "2021-01-01",
        "uuid": "123e4567-e89b-12d3-a456-426614174000",
        "time": "12:34:56.123",
    }
