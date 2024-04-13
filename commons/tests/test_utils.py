from datetime import datetime
from zoneinfo import ZoneInfo

from commons.utilities.date_utils import convert_datetime_to_utc


def test_convert_datetime_to_utc():
    dt = datetime(2021, 1, 1, 0, 0, 0, tzinfo=ZoneInfo("UTC"))
    assert convert_datetime_to_utc(dt) == "2021-01-01T00:00:00+0000"

    dt = datetime(2021, 1, 1, 0, 0, 0)
    assert convert_datetime_to_utc(dt) == "2021-01-01T00:00:00+0000"
