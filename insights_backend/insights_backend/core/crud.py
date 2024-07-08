from sqlalchemy import select

from commons.db.crud import CRUDBase
from insights_backend.core.models import User, UserCreate


class CRUDUser(CRUDBase[User, UserCreate, UserCreate, None]):  # type: ignore
    """
    CRUD for User Model.
    """

    async def get_user_by_email(self, email: str) -> User | None:
        """
        Method to retrieve a single user by email.
        """
        statement = select(User).filter_by(email=email)  # type: ignore
        result = await self.session.execute(statement=statement)
        return result.scalar_one_or_none()
