from sqlalchemy import select

from commons.db.crud import CRUDBase, NotFoundError
from commons.models.tenant import Tenant as TenantCreate, TenantConfig as TenantConfigCreate
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


class CRUDTenant(CRUDBase[Tenant, TenantCreate, TenantCreate, None]):  # type: ignore
    """
    CRUD for Tenant
    """


class CRUDTenantConfig(CRUDBase[TenantConfig, TenantConfigCreate, TenantConfigCreate, None]):  # type: ignore
    """
    CRUD for Tenant Config
    """

    async def get_config_by_tenant_id(self, tenant_id: int) -> TenantConfig:
        """
        Method to retrieve tenant config, based on tenant_id.
        """
        statement = select(TenantConfig).filter_by(tenant_id=tenant_id)  # type: ignore
        result = await self.session.execute(statement=statement)
        config = result.scalar_one_or_none()

        if config is None:
            raise NotFoundError(id=tenant_id)

        return config
