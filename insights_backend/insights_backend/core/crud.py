from sqlalchemy import select, update

from commons.db.crud import CRUDBase, NotFoundError
from commons.models.tenant import CubeConnectionConfig
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

    async def update_tenant_config(self, tenant_id: int, new_config: CubeConnectionConfig) -> TenantConfig | None:
        """
        Method to update the configuration for a tenant. This method retrieves the existing tenant configuration,
        updates the specified fields based on the new configuration provided, and then executes the update statement.
        """
        # Retrieve the existing tenant configuration
        config = await self.get_tenant_config(tenant_id)

        # Update the fields of the existing configuration based on the new configuration provided
        # Update cube_api_url if new_config.cube_api_url is provided
        if new_config.cube_api_url:
            config.cube_connection_config["cube_api_url"] = new_config.cube_api_url  # type: ignore
        # Update cube_auth_type if new_config.cube_auth_type is provided
        if new_config.cube_auth_type:
            config.cube_connection_config["cube_auth_type"] = new_config.cube_auth_type  # type: ignore
        # Update cube_auth_token if new_config.cube_auth_type is "TOKEN" and new_config.cube_auth_token is provided
        if new_config.cube_auth_token and new_config.cube_auth_type == "TOKEN":
            config.cube_connection_config["cube_auth_token"] = new_config.cube_auth_token  # type: ignore
        # Update cube_auth_secret_key if new_config.cube_auth_type is "SECRET_KEY" and
        # new_config.cube_auth_secret_key is provided
        if new_config.cube_auth_secret_key and new_config.cube_auth_type == "SECRET_KEY":
            config.cube_connection_config["cube_auth_secret_key"] = new_config.cube_auth_secret_key  # type: ignore

        # Prepare the update statement to update the tenant configuration
        stmt = (
            update(TenantConfig)
            .filter_by(tenant_id=tenant_id)
            .values(cube_connection_config=config.cube_connection_config)  # type: ignore
        )

        # Execute the update statement to update the tenant configuration in the database
        await self.session.execute(stmt)
        await self.session.commit()

        return config
