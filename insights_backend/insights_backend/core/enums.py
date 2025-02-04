from enum import Enum

from commons.models import BaseModel
from commons.models.enums import Granularity


class StrEnum(str, Enum):
    """
    A string Enum.
    """


class AuthProviders(str, Enum):
    GOOGLE = "google"
    EMAIL = "email"
    OTHER = "other"


class DailySchedule(StrEnum):
    EVERY_DAY = "every_day"
    EVERY_WEEKDAY = "every_weekday"
    EVERY_WEEKEND = "every_weekend"


class WeeklySchedule(StrEnum):
    MONDAY = "monday"
    TUESDAY = "tuesday"
    WEDNESDAY = "wednesday"
    THURSDAY = "thursday"
    FRIDAY = "friday"
    SATURDAY = "saturday"
    SUNDAY = "sunday"


class MonthlySchedule(StrEnum):
    FIRST_DAY = "first_day"
    LAST_DAY = "last_day"
    FIRST_MONDAY = "first_monday"
    LAST_FRIDAY = "last_friday"


class ScheduleOption(BaseModel):
    value: str
    label: str
    description: str | None = None


SCHEDULE_OPTIONS_MAP = {
    Granularity.DAY: [
        ScheduleOption(
            value=DailySchedule.EVERY_DAY.value, label="Every Day", description="Alert will be triggered every day"
        ),
        ScheduleOption(
            value=DailySchedule.EVERY_WEEKDAY.value,
            label="Every Weekday",
            description="Alert will be triggered Monday through Friday",
        ),
        ScheduleOption(
            value=DailySchedule.EVERY_WEEKEND.value,
            label="Every Weekend",
            description="Alert will be triggered Saturday and Sunday",
        ),
    ],
    Granularity.WEEK: [
        ScheduleOption(
            value=day.value,
            label=f"Every {day.value.title()}",
            description=f"Alert will be triggered every {day.value}",
        )
        for day in WeeklySchedule
    ],
    Granularity.MONTH: [
        ScheduleOption(
            value=MonthlySchedule.FIRST_DAY.value,
            label="First day of the month",
            description="Alert will be triggered on the 1st of every month",
        ),
        ScheduleOption(
            value=MonthlySchedule.LAST_DAY.value,
            label="Last day of the month",
            description="Alert will be triggered on the last day of every month",
        ),
        ScheduleOption(
            value=MonthlySchedule.FIRST_MONDAY.value,
            label="First Monday of the month",
            description="Alert will be triggered on the first Monday of every month",
        ),
        ScheduleOption(
            value=MonthlySchedule.LAST_FRIDAY.value,
            label="Last Friday of the month",
            description="Alert will be triggered on the last Friday of every month",
        ),
    ],
}
