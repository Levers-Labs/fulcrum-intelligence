from sqlalchemy import select

from commons.db.crud import CRUDBase, NotFoundError
from insights_backend.core.models import (
    Tenant,
    TenantConfig,
    User,
    UserCreate,
    UserUpdate,
)


class CRUDUser(CRUDBase[User, UserCreate, UserUpdate, None]):  # type: ignore
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

    async def create(self, *, obj_in: UserCreate) -> User:
        values = obj_in.dict()
        # Remove tenant_org_id from values
        values.pop("tenant_org_id")
        obj = self.model(**values)
        self.session.add(obj)
        await self.session.commit()
        await self.session.refresh(obj)
        return obj


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

    async def get_tenant_by_external_id(self, external_id: str) -> Tenant | None:
        """
        Method to retrieve a single tenant by external id.
        """
        statement = select(Tenant).filter_by(external_id=external_id)
        result = await self.session.execute(statement=statement)
        return result.scalar_one_or_none()
