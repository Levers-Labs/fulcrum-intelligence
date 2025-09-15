from sqlalchemy import Select, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import selectinload

from commons.clients.snowflake import SnowflakeClient, SnowflakeConfigModel
from commons.db.crud import CRUDBase, NotFoundError
from commons.exceptions import ConflictError
from commons.models.tenant import SlackConnectionConfig, TenantConfigUpdate
from commons.utilities.context import get_tenant_id
from insights_backend.core.filters import TenantConfigFilter
from insights_backend.core.models import (
    SnowflakeConfig,
    Tenant,
    TenantConfig,
    User,
    UserCreate,
    UserUpdate,
)
from insights_backend.core.models.users import UserTenant
from insights_backend.core.schemas import SnowflakeConfigCreate, SnowflakeConfigUpdate


class CRUDUser(CRUDBase[User, UserCreate, UserUpdate, None]):  # type: ignore
    """
    CRUD for User Model.
    """

    def get_select_query(self) -> Select:
        query = select(User).options(
            selectinload(User.tenant_ids),  # type: ignore
        )
        return query

    async def get_user_by_email(self, email: str) -> User | None:
        """
        Method to retrieve a single user by email.
        """
        statement = self.get_select_query().filter_by(email=email)  # type: ignore
        result = await self.session.execute(statement=statement)
        return result.scalar_one_or_none()

    async def upsert(self, *, obj_in: UserCreate) -> User | None:  # type: ignore
        existing_user = await self.get_user_by_email(obj_in.email)
        if existing_user:
            # Update existing user's attributes
            update_data = obj_in.dict(exclude={"tenant_org_id"})
            for field, value in update_data.items():
                setattr(existing_user, field, value)
            self.session.add(existing_user)
        else:
            # Create new user
            values = obj_in.dict()
            values.pop("tenant_org_id")
            existing_user = self.model(**values)
            self.session.add(existing_user)

        await self.session.flush()
        # Upsert user-tenant
        stmt = (
            insert(UserTenant)
            .values(user_id=existing_user.id, tenant_id=get_tenant_id())  # type: ignore
            .on_conflict_do_nothing(constraint="uq_user_tenant")
        )
        await self.session.execute(stmt)
        await self.session.commit()
        await self.session.refresh(existing_user)
        return existing_user

    async def update(self, *, obj: User, obj_in: UserUpdate) -> User:  # type: ignore
        """Update user details."""
        # If email is being updated, check if new email already exists
        if obj_in.email != obj.email:
            existing_user = await self.get_user_by_email(obj_in.email)
            if existing_user:
                raise ValueError("Email already exists")

        # Update user attributes
        update_data = obj_in.model_dump(exclude_unset=True)
        for field, value in update_data.items():
            setattr(obj, field, value)

        self.session.add(obj)
        await self.session.commit()
        await self.session.refresh(obj)
        return obj


class TenantCRUD(CRUDBase[Tenant, Tenant, Tenant, TenantConfigFilter]):  # type: ignore
    """
    CRUD for Tenant Model.
    """

    filter_class = TenantConfigFilter

    def get_select_query(self) -> Select:
        """
        Override base query to include TenantConfig join for filtering.
        """
        query = select(Tenant).outerjoin(TenantConfig, Tenant.id == TenantConfig.tenant_id)
        return query

    async def get(self, id: int) -> Tenant:
        """
        Override get method to explicitly filter on Tenant.id to avoid column ambiguity with joins.
        """
        statement = self.get_select_query().where(Tenant.id == id)
        results = await self.session.execute(statement=statement)
        instance: Tenant | None = results.unique().scalar_one_or_none()

        if instance is None:
            raise NotFoundError(id=id)

        return instance

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

    # Snowflake Configuration Methods
    async def get_snowflake_config(self) -> SnowflakeConfig:
        """
        Method to retrieve the Snowflake config for a tenant.
        """
        # Get the current tenant ID from context
        tenant_id = get_tenant_id()

        # Build SQLAlchemy select statement to find Snowflake config by tenant_id
        statement = select(SnowflakeConfig).filter_by(tenant_id=tenant_id)
        result = await self.session.execute(statement=statement)
        instance: SnowflakeConfig | None = result.scalar_one_or_none()

        # Raise NotFoundError if no configuration exists for this tenant
        if instance is None:
            raise NotFoundError(id=tenant_id)
        return instance

    async def create_snowflake_config(self, config_data: SnowflakeConfigCreate) -> SnowflakeConfig:
        """
        Creates a new Snowflake configuration for a tenant.
        Sensitive credentials are automatically encrypted by EncryptedType.
        """
        # Get the current tenant ID from context
        tenant_id = get_tenant_id()
        if tenant_id is None:
            raise ValueError("Tenant ID is required")

        # Convert Pydantic model to dictionary for database insertion
        config_data_dict = config_data.model_dump(mode="json")
        snowflake_config = SnowflakeConfig(
            tenant_id=tenant_id,
            **config_data_dict,
        )

        # Persist the new configuration to database
        self.session.add(snowflake_config)
        try:
            await self.session.commit()
        except IntegrityError as e:
            # Check if this is a unique constraint violation for tenant_id
            if "uq_snowflake_config_tenant_id" in str(e) or "tenant_id" in str(e.orig):
                raise ConflictError("Snowflake configuration already exists for this tenant") from e
            raise
        await self.session.refresh(snowflake_config)
        return snowflake_config

    async def update_snowflake_config(self, config_data: SnowflakeConfigUpdate) -> SnowflakeConfig:
        """
        Updates an existing Snowflake configuration for a tenant.
        Sensitive credentials are automatically encrypted by EncryptedType.
        """
        # Retrieve existing configuration first
        snowflake_config = await self.get_snowflake_config()

        # Convert update data to dictionary, excluding unset fields
        update_data = config_data.model_dump(exclude_unset=True, mode="json")

        # Update only the fields that were provided in the update request
        for field, value in update_data.items():
            if hasattr(snowflake_config, field):
                setattr(snowflake_config, field, value)

        # Persist the updated configuration to database
        self.session.add(snowflake_config)
        await self.session.commit()
        await self.session.refresh(snowflake_config)
        return snowflake_config

    async def delete_snowflake_config(self) -> bool:
        """
        Deletes the Snowflake configuration for a tenant.
        """
        try:
            # Retrieve the configuration to delete
            snowflake_config = await self.get_snowflake_config()

            # Remove from database
            await self.session.delete(snowflake_config)
            await self.session.commit()
            return True
        except NotFoundError:
            # Return False if no configuration exists to delete
            return False

    async def test_snowflake_connection(self, config_data: dict) -> dict:
        """
        Tests a Snowflake connection with the provided configuration.
        Returns a dictionary with success status and details.

        Args:
            config_data: Dictionary with Snowflake configuration parameters
        """
        try:
            # Create a SnowflakeConfigModel from the provided configuration
            client_config = SnowflakeConfigModel(**config_data)

            # Use the SnowflakeClient to test the connection
            client = SnowflakeClient(config=client_config)
            return await client.test_connection()
        except Exception as e:
            # Handle any other unexpected errors during connection testing
            return {"success": False, "message": f"Connection failed: {str(e)}", "connection_details": None}

    async def enable_metric_cache(self, enabled: bool = True) -> TenantConfig:
        """
        Enables or disables the metric cache feature for a tenant.
        """
        # Get the current tenant ID from context
        tenant_id = get_tenant_id()
        if tenant_id is None:
            raise ValueError("Tenant ID is required")

        # Retrieve and update the tenant configuration
        tenant_config = await self.get_tenant_config(tenant_id)
        tenant_config.enable_metric_cache = enabled

        # Persist the updated configuration to database
        self.session.add(tenant_config)
        await self.session.commit()
        return tenant_config
