from sqlalchemy import (
    Select,
    and_,
    delete,
    func,
    select,
)
from sqlalchemy.orm import selectinload

from commons.db.crud import CRUDBase, NotFoundError
from commons.db.filters import BaseFilter
from commons.models.enums import Granularity
from commons.utilities.context import get_tenant_id
from query_manager.core.filters import DimensionFilter, MetricCacheConfigFilter, MetricFilter
from query_manager.core.models import (
    Dimension,
    Metric,
    MetricCacheConfig,
    MetricCacheGrainConfig,
    MetricComponent,
    MetricDimension,
    MetricInfluence,
    MetricInput,
)
from query_manager.core.schemas import MetricCacheConfigRead
from query_manager.semantic_manager.models import MetricSyncStatus, SyncOperation


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
        statement = self.get_select_query().filter(func.lower(Metric.metric_id) == metric_id.lower())
        results = await self.session.execute(statement=statement)
        instance: Metric | None = results.unique().scalar_one_or_none()

        if instance is None:
            raise NotFoundError(id=metric_id)

        return instance

    async def get_all_metric_ids(self) -> set[str]:
        statement = select(Metric.metric_id)  # type: ignore
        results = await self.session.execute(statement=statement)
        return {metric_id for metric_id, in results}

    async def delete_metric(self, metric_id: str) -> None:
        """Delete a metric and its relationships."""
        metric = await self.get_by_metric_id(metric_id)

        # Delete from association tables and notifications
        association_tables = [
            (MetricDimension, [MetricDimension.metric_id]),
            (MetricInfluence, [MetricInfluence.influencer_id, MetricInfluence.influenced_id]),
            (MetricComponent, [MetricComponent.parent_id, MetricComponent.component_id]),
            (MetricInput, [MetricInput.metric_id, MetricInput.input_id]),
        ]

        for table_class, columns in association_tables:
            for column in columns:
                await self.session.execute(delete(table_class).where(column == metric.id))  # type: ignore

        # Delete the metric
        await self.session.execute(delete(Metric).where(Metric.id == metric.id))  # type: ignore

        await self.session.commit()


class CRUDMetricCacheGrainConfig(
    CRUDBase[MetricCacheGrainConfig, MetricCacheGrainConfig, MetricCacheGrainConfig, BaseFilter]
):  # noqa
    """
    CRUD operations for MetricCacheGrainConfig Model.
    Handles configuration for Snowflake sync at different grain levels (day/week/month).
    """

    async def get_by_grain(self, grain: Granularity) -> MetricCacheGrainConfig:
        """
        Get grain configuration by grain type for a specific tenant.
        """
        statement = select(MetricCacheGrainConfig).filter_by(grain=grain)

        result = await self.session.execute(statement)
        instance: MetricCacheGrainConfig | None = result.scalar_one_or_none()

        if instance is None:
            raise NotFoundError(id=f"{grain}")
        return instance

    async def get_enabled_grains(self) -> list[MetricCacheGrainConfig]:
        """
        Get all enabled grain cache configurations for a tenant.
        """
        statement = select(MetricCacheGrainConfig).filter_by(is_enabled=True)

        result = await self.session.execute(statement)
        return list(result.scalars().all())

    async def create_default_grain_configs(self) -> list[MetricCacheGrainConfig]:
        """
        Create default grain configurations for a new tenant.
        Sets up standard sync periods for day, week, and month granularities.
        """

        # Define default configurations with appropriate sync periods
        default_configs = [
            {
                "grain": Granularity.DAY,
                "is_enabled": True,
                "initial_sync_period": 730,  # 2 years for daily data
                "delta_sync_period": 90,  # 90 days for delta sync
            },
            {
                "grain": Granularity.WEEK,
                "is_enabled": True,
                "initial_sync_period": 1095,  # 3 years for weekly data
                "delta_sync_period": 120,  # 120 days for delta sync
            },
            {
                "grain": Granularity.MONTH,
                "is_enabled": True,
                "initial_sync_period": 1825,  # 5 years for monthly data
                "delta_sync_period": 180,  # 180 days for delta sync
            },
        ]

        configs = []
        # Create and add each configuration to the session
        for config_data in default_configs:
            # Create only if it doesn't exist
            try:
                grain_config = await self.get_by_grain(config_data["grain"])  # type: ignore
                configs.append(grain_config)
            except NotFoundError:
                grain_config = MetricCacheGrainConfig(**config_data)
                self.session.add(grain_config)
                configs.append(grain_config)

        # Commit all configurations at once
        await self.session.commit()

        # Refresh each config to get the generated IDs
        for config in configs:
            await self.session.refresh(config)

        return configs

    async def update_grain_config(self, grain: Granularity, update_data: dict) -> MetricCacheGrainConfig:
        """
        Update a specific grain configuration.
        """
        # Get the existing configuration
        grain_config = await self.get_by_grain(grain)

        # Update only the provided fields
        for field, value in update_data.items():
            if hasattr(grain_config, field):
                setattr(grain_config, field, value)

        # Save changes to database
        self.session.add(grain_config)
        await self.session.commit()
        await self.session.refresh(grain_config)
        return grain_config

    async def bulk_update_grain_configs(self, configs_data: list[dict]) -> list[MetricCacheGrainConfig]:
        """
        Bulk update multiple grain configurations using upsert pattern.
        """
        updated_configs = []

        for config_data in configs_data:
            # Extract grain from config data
            grain = config_data.pop("grain")

            # Only update if there's actual data to update
            if config_data:
                try:
                    # Try to get existing configuration
                    config = await self.get_by_grain(grain)
                    # Update only the provided fields
                    for field, value in config_data.items():
                        if hasattr(config, field):
                            setattr(config, field, value)
                    self.session.add(config)
                except NotFoundError:
                    # Create new configuration if none exists
                    config = MetricCacheGrainConfig(grain=grain, **config_data)  # type: ignore
                    self.session.add(config)

                updated_configs.append(config)

        # Commit all changes at once
        await self.session.commit()

        # Refresh each config to get updated data
        for config in updated_configs:
            await self.session.refresh(config)

        return updated_configs


class CRUDMetricCacheConfig(
    CRUDBase[MetricCacheConfig, MetricCacheConfig, MetricCacheConfig, MetricCacheConfigFilter]
):  # noqa
    """
    CRUD operations for MetricCacheConfig Model.
    Handles configuration for individual metric caching to Snowflake.
    """

    filter_class = MetricCacheConfigFilter

    async def get_by_metric_id(self, metric_id: str) -> MetricCacheConfigRead:
        """
        Get cache configuration for a specific metric.
        """
        statement = select(MetricCacheConfig).filter_by(metric_id=metric_id)

        result = await self.session.execute(statement)
        config: MetricCacheConfig | None = result.scalar_one_or_none()

        if config is None:
            raise NotFoundError(id=metric_id)

        config_read = MetricCacheConfigRead.model_validate(config, from_attributes=True)

        # Get sync information for this metric
        tenant_id = get_tenant_id()

        # Query 1: Get latest sync info
        latest_sync_query = (
            select(MetricSyncStatus.last_sync_at, MetricSyncStatus.sync_status)  # type: ignore
            .where(
                and_(
                    MetricSyncStatus.tenant_id == tenant_id,  # type: ignore
                    MetricSyncStatus.metric_id == metric_id,  # type: ignore
                    MetricSyncStatus.sync_operation == SyncOperation.SNOWFLAKE_CACHE,  # type: ignore
                )
            )
            .order_by(MetricSyncStatus.last_sync_at.desc())  # type: ignore
            .limit(1)
        )

        # Query 2: Get date range across all syncs
        snapshot_range_query = select(
            func.min(MetricSyncStatus.first_snapshot_date).label("first_snapshot_date"),
            func.max(MetricSyncStatus.last_snapshot_date).label("last_snapshot_date"),
        ).where(
            and_(
                MetricSyncStatus.tenant_id == tenant_id,  # type: ignore
                MetricSyncStatus.metric_id == metric_id,  # type: ignore
                MetricSyncStatus.sync_operation == SyncOperation.SNOWFLAKE_CACHE,  # type: ignore
            )
        )

        # Execute both queries
        sync_result = await self.session.execute(latest_sync_query)
        sync_info = sync_result.mappings().first()

        snapshot_result = await self.session.execute(snapshot_range_query)
        snapshot_info = snapshot_result.mappings().first()

        if sync_info:
            config_read.last_sync_date = sync_info.last_sync_at
            config_read.sync_status = sync_info.sync_status
            config_read.first_snapshot_date = snapshot_info.first_snapshot_date  # type: ignore
            config_read.last_snapshot_date = snapshot_info.last_snapshot_date  # type: ignore

        return config_read

    async def create_or_update_metric_config(self, metric_id: str, is_enabled: bool) -> MetricCacheConfigRead:
        """
        Create or update cache configuration for a specific metric.
        Uses upsert pattern to handle both creation and updates.
        """
        try:
            # Try to get existing configuration
            statement = select(MetricCacheConfig).filter_by(metric_id=metric_id)

            result = await self.session.execute(statement)
            config: MetricCacheConfig | None = result.scalar_one_or_none()

            if config is None:
                raise NotFoundError(id=metric_id)

            config.is_enabled = is_enabled
            self.session.add(config)
        except NotFoundError:
            # Create new configuration if none exists
            config = MetricCacheConfig(metric_id=metric_id, is_enabled=is_enabled)  # type: ignore
            self.session.add(config)

        # Commit changes and refresh to get updated data
        await self.session.commit()
        return await self.get_by_metric_id(metric_id)

    async def bulk_update_metric_configs(self, metric_ids: list[str], is_enabled: bool) -> list[MetricCacheConfigRead]:
        """
        Bulk update cache configurations for multiple metrics.
        """
        updated_configs = []

        # Process each metric ID individually
        for metric_id in metric_ids:
            config = await self.create_or_update_metric_config(metric_id, is_enabled)
            updated_configs.append(config)

        return updated_configs

    async def enable_all_metrics(self) -> list[MetricCacheConfigRead]:
        """
        Enable caching for all metrics of a tenant.
        """
        # Get all metric IDs for the tenant
        metric_statement = select(Metric.metric_id)  # type: ignore
        metric_result = await self.session.execute(metric_statement)
        metric_ids = [metric_id for metric_id, in metric_result]

        # Enable caching for all found metrics
        return await self.bulk_update_metric_configs(metric_ids, True)

    async def get_enabled_metrics(self) -> list[MetricCacheConfig]:
        """
        Get all enabled metric cache configurations.
        """
        # Filter for enabled configurations and include metric relationship
        statement = select(MetricCacheConfig).filter_by(is_enabled=True)  # type: ignore

        result = await self.session.execute(statement)
        return list(result.scalars().all())
