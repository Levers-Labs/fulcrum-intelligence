from enum import Enum


class NotificationChannel(str, Enum):
    SLACK = "slack"
    EMAIL = "email"
