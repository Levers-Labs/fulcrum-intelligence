from sqlalchemy import Select, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import selectinload

from commons.db.crud import CRUDBase, NotFoundError
from commons.utilities.context import get_tenant_id
from query_manager.core.filters import DimensionFilter, MetricFilter, MetricNotificationsFilter
from query_manager.core.models import Dimension, Metric, MetricNotifications


class CRUDDimensions(CRUDBase[Dimension, Dimension, Dimension, DimensionFilter]):  # noqa
    """
    CRUD for Dimension Model.
    """

    filter_class = DimensionFilter

    async def get_by_dimension_id(self, dimension_id: str) -> Dimension:
        statement = self.get_select_query().filter_by(dimension_id=dimension_id)
        results = await self.session.execute(statement=statement)
        instance: Dimension | None = results.unique().scalar_one_or_none()

        if instance is None:
            raise NotFoundError(id=dimension_id)
        return instance


class CRUDMetric(CRUDBase[Metric, Metric, Metric, MetricFilter]):  # noqa
    """
    CRUD for Metric Model.
    """

    filter_class = MetricFilter

    def get_select_query(self) -> Select:
        query = select(Metric).options(
            selectinload(Metric.dimensions),  # type: ignore
            selectinload(Metric.influences),  # type: ignore
            selectinload(Metric.influencers),  # type: ignore
            selectinload(Metric.inputs),  # type: ignore
            selectinload(Metric.outputs),  # type: ignore
        )
        return query

    async def get_by_metric_id(self, metric_id: str) -> Metric:
        statement = self.get_select_query().filter_by(metric_id=metric_id)
        results = await self.session.execute(statement=statement)
        instance: Metric | None = results.unique().scalar_one_or_none()

        if instance is None:
            raise NotFoundError(id=metric_id)

        return instance


class CRUDMetricNotifications(
    CRUDBase[MetricNotifications, MetricNotifications, MetricNotifications, MetricNotificationsFilter]  # type: ignore
):
    """
    CRUD operations for MetricNotifications model.
    """

    filter_class = MetricNotificationsFilter

    async def create_metric_notifications(
        self, metric_id: int, slack_enabled: bool, slack_channels: dict
    ) -> MetricNotifications:
        """
        Creates or updates MetricNotifications for a given metric_id.

        Args:
            metric_id (int): The ID of the metric for which to create or update notifications.
            slack_enabled (bool): Indicates if Slack notifications are enabled.
            slack_channels (dict): A dictionary containing Slack channel IDs and names.

        Returns:
            MetricNotifications: The created or updated MetricNotifications instance.
        """
        tenant_id = get_tenant_id()
        data = {
            "metric_id": metric_id,
            "slack_enabled": slack_enabled,
            "slack_channels": slack_channels,
            "tenant_id": tenant_id,
        }

        # Prepare the insert statement with on conflict handling
        stmt = (
            insert(MetricNotifications)
            .values(**data)
            .on_conflict_do_update(
                index_elements=["metric_id", "tenant_id"],
                set_={
                    "slack_enabled": slack_enabled,
                    "slack_channels": slack_channels,
                },
            )
        )

        # Execute the statement and get the result
        await self.session.execute(stmt)
        await self.session.commit()
        obj = MetricNotifications(**data)
        return obj

    async def get_metric_notifications(self, metric_id: int) -> MetricNotifications | None:
        """
        Retrieves MetricNotifications for a given metric_id.

        Args:
            metric_id (int): The ID of the metric for which to retrieve notifications.

        Returns:
            MetricNotifications | None: The MetricNotifications instance if found, otherwise None.
        """
        statement = self.get_select_query().filter_by(metric_id=metric_id)
        result = await self.session.execute(statement=statement)
        instance: MetricNotifications | None = result.unique().scalar_one_or_none()  # noqa

        return instance
