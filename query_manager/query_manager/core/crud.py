from sqlalchemy import (
    Select,
    delete,
    func,
    select,
)
from sqlalchemy.orm import selectinload

from commons.db.crud import CRUDBase, NotFoundError
from query_manager.core.filters import DimensionFilter, MetricFilter
from query_manager.core.models import (
    Dimension,
    Metric,
    MetricComponent,
    MetricDimension,
    MetricInfluence,
    MetricInput,
)


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
