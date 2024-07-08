from enum import Enum


class AuthProviders(str, Enum):
    GOOGLE = "google"
    EMAIL = "email"
    OTHER = "other"
