from sqlalchemy import select, update

from commons.db.crud import CRUDBase, NotFoundError
from commons.models.tenant import SlackConnectionConfig, TenantConfigUpdate
from insights_backend.core.filters import TenantConfigFilter
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


class TenantCRUD(CRUDBase[Tenant, Tenant, Tenant, TenantConfigFilter]):  # type: ignore
    """
    CRUD for Tenant Model.
    """

    filter_class = TenantConfigFilter

    async def get_tenant_config(self, tenant_id: int) -> TenantConfig:
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

    async def get_tenant(self, identifier: str) -> Tenant | None:
        """
        Method to retrieve a single tenant by external id.
        """

        statement = select(Tenant).filter_by(identifier=identifier)
        result = await self.session.execute(statement=statement)
        instance: Tenant | None = result.scalar_one_or_none()

        if instance is None:
            raise NotFoundError(id=identifier)

        return instance

    async def update_tenant_config(self, tenant_id: int, new_config: TenantConfigUpdate) -> TenantConfig:
        """
        Updates the configuration for a tenant based on the provided new configuration. This method first converts the
        new configuration into a dictionary,
        then prepares and executes an update statement to modify the tenant configuration in the database. Finally, it
        retrieves and returns the updated tenant configuration.
        """

        # Convert the new configuration into a dictionary for easier manipulation
        new_config_dict = new_config.model_dump(mode="json")

        # Prepare the update statement to modify the tenant configuration
        update_stmt = (
            update(TenantConfig)
            .filter_by(tenant_id=tenant_id)
            .values(
                cube_connection_config=new_config_dict.get("cube_connection_config", {}),
                enable_story_generation=new_config_dict.get("enable_story_generation"),
            )
        )

        # Execute the update statement to modify the tenant configuration in the database
        await self.session.execute(update_stmt)
        await self.session.commit()

        # Retrieve the updated tenant configuration
        updated_config = await self.get_tenant_config(tenant_id)

        return updated_config

    async def update_slack_connection(self, tenant_id: int, slack_config: SlackConnectionConfig) -> TenantConfig:
        """
        Updates the Slack connection details for a tenant.
        Finds the tenant config,
        updates the Slack connection details,
        and saves the updated tenant config.
        """
        # Find the tenant config
        tenant_config = await self.get_tenant_config(tenant_id)

        # Update the tenant config with the new Slack connection details
        tenant_config.slack_connection = slack_config.model_dump(mode="json")  # type: ignore

        # Save the updated tenant config
        self.session.add(tenant_config)
        await self.session.commit()
        await self.session.refresh(tenant_config)

        return tenant_config

    async def revoke_slack_connection(self, tenant_config: TenantConfig) -> TenantConfig:
        """
        Revokes the Slack connection for a tenant.
        """
        # clear the slack connection details
        tenant_config.slack_connection = None
        # save the updated tenant config
        self.session.add(tenant_config)
        await self.session.commit()
        await self.session.refresh(tenant_config)
        return tenant_config
