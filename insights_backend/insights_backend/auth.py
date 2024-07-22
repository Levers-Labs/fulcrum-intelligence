from commons.auth.auth import Oauth2Auth
from insights_backend.core.crud import CRUDUser


class InsightsBackendAuth(Oauth2Auth):
    def __init__(self, user_crud: CRUDUser, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.user_crud = user_crud

    async def get_app_user(self, user_id: int, token: str):  # noqa
        """
        Method to fetch the user from DB, whose ID is present in the JWT.
        Input:
        - user_id: id of the user
        - token : jwt token
        """
        user = await self.user_crud.get(id=user_id)
        return user.model_dump() if user else None
