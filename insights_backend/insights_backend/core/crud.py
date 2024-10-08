from sqlalchemy import select

from commons.db.crud import CRUDBase, NotFoundError
from insights_backend.core.models import (
    Tenant,
    TenantConfig,
    User,
    UserCreate,
)


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


class TenantCRUD(CRUDBase[Tenant, Tenant, Tenant, None]):  # type: ignore
    """
    CRUD for Tenant Model.
    """

    async def get_tenant_config(self, tenant_id: int) -> TenantConfig | None:
        """
        Method to retrieve the config for a tenant.
        """
        statement = select(TenantConfig).filter_by(tenant_id=tenant_id)
        results = await self.session.execute(statement=statement)
        instance: TenantConfig | None = results.unique().scalar_one_or_none()

        if instance is None:
            raise NotFoundError(id=tenant_id)
        return instance
