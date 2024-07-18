from commons.auth.auth import Auth
from commons.db.session import get_async_session
from insights_backend.config import get_settings
from insights_backend.core.crud import CRUDUser
from insights_backend.core.models import User


class AuthExtended(Auth):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def get_auth_user(self, user_id: int, token: str):
        """
        Method to fetch the user from DB, whose ID is present in the JWT.
        Input:
        - user_id: id of the user
        - token : jwt token
        """
        settings = get_settings()
        async for session in get_async_session(database_url=settings.DATABASE_URL, options={}):
            user_crud_client = CRUDUser(model=User, session=session)
            return await user_crud_client.get(id=user_id)
