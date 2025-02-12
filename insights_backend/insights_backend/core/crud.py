from typing import Any

from sqlalchemy import (
    delete,
    func,
    select,
    update,
)

from commons.db.crud import CRUDBase, NotFoundError
from commons.models.tenant import SlackConnectionConfig, TenantConfigUpdate
from commons.utilities.context import get_tenant_id
from insights_backend.core.filters import TenantConfigFilter
from insights_backend.core.models import (
    Alert,
    Tenant,
    TenantConfig,
    User,
    UserCreate,
    UserUpdate,
)
from insights_backend.core.models.notifications import NotificationChannelConfig, NotificationType
from insights_backend.core.schemas import (
    AlertCreateRequest,
    AlertPublishRequest,
    AlertUpdateRequest,
    NotificationChannelDetail,
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


class CRUDNotificationChannel(CRUDBase[NotificationChannelConfig, NotificationChannelDetail, None, None]):
    """
    CRUD operations for NotificationChannelConfig model.
    This class provides methods for creating, retrieving, updating, and deleting notification channels.
    """

    async def create(
        self,
        *,
        notification_configs: list[NotificationChannelDetail],
        alert_id: int | None = None,
        # report_id: int | None = None
    ) -> list[NotificationChannelConfig]:
        """
        Creates notification channels for alerts or reports.

        Args:
            notification_configs: List of notification channel configurations
            alert_id: Optional ID of the alert to associate with
            # report_id: Optional ID of the report to associate with

        Returns:
            List of created NotificationChannelConfig objects
        """
        channels = []
        for config in notification_configs:

            # Create a new NotificationChannelConfig object
            channel = NotificationChannelConfig(
                channel_type=config.channel_type,
                recipients=config.model_dump()["recipients"],
                template="",
                alert_id=alert_id,
                # report_id=report_id
            )
            self.session.add(channel)
            channels.append(channel)
        # Commit the session to save all created channels
        await self.session.commit()
        # Retrieve all created channels in one query
        result = await self.session.execute(
            select(NotificationChannelConfig).filter(NotificationChannelConfig.alert_id == alert_id)
        )
        return list(result.scalars().all())

    async def get_channels_by_alert(self, alert_id: int) -> list[NotificationChannelConfig]:
        """Retrieves notification channels for an alert"""
        result = await self.session.execute(
            select(NotificationChannelConfig).filter(NotificationChannelConfig.alert_id == alert_id)
        )
        return list(result.scalars().all())

    async def delete_by_alert_ids(self, alert_ids: int | list[int]) -> None:
        """Deletes all notification channels for one or multiple alerts"""
        # Convert single ID to list if needed
        ids = [alert_ids] if isinstance(alert_ids, int) else alert_ids

        # Execute the delete statement
        await self.session.execute(delete(NotificationChannelConfig).where(NotificationChannelConfig.alert_id.in_(ids)))
        # Commit the session to save the changes
        await self.session.commit()

    async def update_by_alert_id(
        self, *, alert_id: int, notification_configs: list[NotificationChannelDetail]
    ) -> list[NotificationChannelConfig]:
        """Updates notification channels for an alert"""
        # Delete existing channels
        await self.session.execute(
            delete(NotificationChannelConfig).where(NotificationChannelConfig.alert_id == alert_id)
        )
        await self.session.commit()

        # Create new channels
        channels = []
        for config in notification_configs:

            channel = NotificationChannelConfig(
                channel_type=config.channel_type,
                recipients=config.model_dump()["recipients"],
                template="",
                alert_id=alert_id,
            )
            self.session.add(channel)
            channels.append(channel)

        await self.session.commit()

        # Return the newly created channels
        result = await self.session.execute(
            select(NotificationChannelConfig).filter(NotificationChannelConfig.alert_id == alert_id)
        )
        return list(result.scalars().all())


class CRUDAlert(CRUDBase[Alert, AlertCreateRequest, None, None]):
    """
    CRUD operations for the Alert model.
    """

    async def get(self, alert_id: int) -> Alert | None:
        """
        Retrieves a single Alert by its ID.

        :param alert_id: The ID of the Alert to retrieve.
        :return: The Alert object if found, otherwise None.
        """
        # Construct the SQL statement to select the Alert by ID
        statement = select(Alert).filter_by(id=alert_id)
        # Execute the statement and return the result
        result = await self.session.execute(statement=statement)
        return result.scalar_one_or_none()

    async def create(self, *, new_alert: AlertCreateRequest) -> Any:
        """Creates a new Alert with its associated notification channels."""

        alert_data = new_alert.model_dump(exclude={"trigger", "notification_channels"}, exclude_unset=True)

        alert = Alert(
            **alert_data,
            type=NotificationType.ALERT,
            tenant_id=get_tenant_id(),
            summary=str(new_alert.trigger.condition),
            trigger={
                "type": new_alert.trigger.type,
                "condition": new_alert.trigger.condition.model_dump(exclude_unset=True),
            },
        )

        # Add the new Alert to the session
        self.session.add(alert)
        # Flush the session to save the changes
        await self.session.flush()
        # Commit the session to persist the changes
        await self.session.commit()
        await self.session.refresh(alert)
        return alert

    async def delete(self, ids: int | list[int]) -> None:
        """Deletes one or multiple Alerts by their IDs."""
        # Convert a single ID to a list if needed
        alert_ids = [ids] if isinstance(ids, int) else ids

        # Construct the SQL statement to delete Alerts by IDs
        await self.session.execute(delete(Alert).where(Alert.id.in_(alert_ids)))
        # Commit the session to save the changes
        await self.session.commit()

    async def update_active_status(self, alert_ids: list[int], is_active: bool) -> None:
        """Updates the active status of one or multiple Alerts."""

        # Construct the SQL statement to update Alerts' active status
        await self.session.execute(update(Alert).where(Alert.id.in_(alert_ids)).values(is_active=is_active))
        # Commit the session to save the changes
        await self.session.commit()

    async def update(
        self,
        *,
        alert_id: int,
        update_alert: AlertUpdateRequest,
    ) -> Alert:
        """Updates an existing Alert"""
        alert = await self.get(alert_id)
        if not alert:
            raise ValueError(f"Alert with id {alert_id} not found")

        # Don't allow unpublishing through update
        # if not update_alert.is_published and alert.is_published:
        #     raise ValueError("Cannot unpublish alert through update. Use dedicated unpublish endpoint")

        update_data = update_alert.model_dump(exclude_unset=True, exclude={"notification_channels"})
        trigger = update_data.get("trigger")
        if trigger:
            update_data["trigger"] = {"type": trigger["type"], "condition": trigger["condition"]}
            update_data["summary"] = str(trigger["condition"])

        result = await self.session.execute(
            update(Alert).where(Alert.id == alert_id).values(**update_data).returning(Alert)
        )
        await self.session.commit()

        return result.scalar_one()

    async def publish(
        self,
        *,
        alert_id: int,
        publish_data: AlertPublishRequest,
    ) -> Alert:
        """Publish a draft alert with notification channels"""
        alert = await self.get(alert_id)
        if not alert:
            raise ValueError(f"Alert with id {alert_id} not found")

        if alert.is_published:
            raise ValueError("Alert is already published")

        if not publish_data.notification_channels:
            raise ValueError("Notification channels are required for publishing")

        # Update alert with publish data
        update_data = publish_data.model_dump(exclude_unset=True, exclude={"notification_channels"})

        # Ensure is_published is True
        update_data["is_published"] = True

        result = await self.session.execute(
            update(Alert).where(Alert.id == alert_id).values(**update_data).returning(Alert)
        )

        return result.scalar_one()

    async def get_unique_tags(self) -> list[str]:
        """
        Retrieve unique tags across all alerts.

        Returns:
            List of unique tags sorted alphabetically
        """
        # Using func.unnest to flatten the tags array
        statement = (
            select(func.unnest(Alert.tags).label("tag"))
            .where(Alert.tags.is_not(None))
            .where(Alert.tenant_id == get_tenant_id())
            .distinct()
            .order_by("tag")
        )

        result = await self.session.execute(statement)
        return [row[0] for row in result.fetchall()]

    async def get_missing_ids(self, ids: list[int]) -> set[int]:
        """
        Find which IDs from the input list don't exist in the database.

        Args:
            ids: List of alert IDs to check

        Returns:
            Set of IDs that don't exist in the database
        """
        statement = select(Alert.id).where(Alert.id.in_(ids))
        result = await self.session.execute(statement)
        found_ids = {row[0] for row in result.fetchall()}
        return set(ids) - found_ids
