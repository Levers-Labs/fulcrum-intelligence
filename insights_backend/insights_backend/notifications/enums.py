from enum import Enum


class NotificationType(str, Enum):
    """Types of notifications supported by the system"""

    ALERT = "ALERT"  # For real-time alerts and notifications
    REPORT = "REPORT"  # For scheduled reports


class TriggerType(str, Enum):
    """Types of triggers that can generate notifications"""

    METRIC_STORY = "METRIC_STORY"  # Story-based triggers for metric insights
    METRIC_THRESHOLD = "METRIC_THRESHOLD"  # Threshold-based triggers for metric monitoring


class Comparisons(str, Enum):
    """Types of comparisons available for metric analysis"""

    # TODO: check which all to be added
    PERCENTAGE_CHANGE = "PERCENTAGE_CHANGE"  # Relative change in percentage
    ABSOLUTE_CHANGE = "ABSOLUTE_CHANGE"  # Absolute numerical change


class ExecutionStatus(str, Enum):
    """Status of the notification execution"""

    # None -> Scheduled -> Pending | Late -> Running -> Completed | Failed | Crashed

    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CRASHED = "CRASHED"


class DayOfWeek(str, Enum):
    MONDAY = "MON"
    TUESDAY = "TUE"
    WEDNESDAY = "WED"
    THURSDAY = "THU"
    FRIDAY = "FRI"
    SATURDAY = "SAT"
    SUNDAY = "SUN"


class Month(str, Enum):
    JANUARY = "JAN"
    FEBRUARY = "FEB"
    MARCH = "MAR"
    APRIL = "APR"
    MAY = "MAY"
    JUNE = "JUN"
    JULY = "JUL"
    AUGUST = "AUG"
    SEPTEMBER = "SEP"
    OCTOBER = "OCT"
    NOVEMBER = "NOV"
    DECEMBER = "DEC"
