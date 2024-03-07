from app.core.models import UserRead
from app.db.models import CustomBase


class UserList(CustomBase):
    count: int
    results: list[UserRead]
