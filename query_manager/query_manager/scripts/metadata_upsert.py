import asyncio
import json
import logging

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload, sessionmaker

from query_manager.config import get_settings
from query_manager.core.models import Dimension, Metric

# Set up logging
logger = logging.getLogger(__name__)


async def upsert_data(session: AsyncSession, dimensions_file_path: str, metrics_file_path: str) -> None:
    """
    upsert dimensions and metrics data from JSON files into the database.

    Args:
        session (AsyncSession): The database session.
        dimensions_file_path (str): Path to the dimensions JSON file.
        metrics_file_path (str): Path to the metrics JSON file.
    """
    # Upsert Dimensions
    with open(dimensions_file_path) as f:
        dimensions_data = json.load(f)

    logger.info(f"Upserting {len(dimensions_data)} dimensions...")
    for i, dim_data in enumerate(dimensions_data, 1):
        defaults = {
            "label": dim_data["label"],
            "reference": dim_data.get("reference"),
            "definition": dim_data.get("definition"),
            "meta_data": dim_data["metadata"],
        }
        stmt = insert(Dimension).values(dimension_id=dim_data["id"], **defaults)
        stmt = stmt.on_conflict_do_update(index_elements=["dimension_id"], set_=defaults)
        await session.execute(stmt)
        if i % 10 == 0 or i == len(dimensions_data):
            logger.info(f"Processed {i}/{len(dimensions_data)} dimensions")
    await session.commit()
    logger.info("Dimensions upsert completed")

    # Refresh dimensions
    dimensions = await session.scalars(select(Dimension))
    dimension_id_map = {dim.dimension_id: dim for dim in dimensions}

    # Upsert Metrics
    with open(metrics_file_path) as file:
        metrics_data = json.load(file)

    logger.info(f"Upsetting {len(metrics_data)} metrics...")
    for i, metric_data in enumerate(metrics_data, 1):
        defaults = {
            "label": metric_data["label"],
            "abbreviation": metric_data.get("abbreviation"),
            "definition": metric_data.get("definition"),
            "unit_of_measure": metric_data.get("unit_of_measure"),
            "unit": metric_data.get("unit"),
            "terms": metric_data.get("terms") or [],
            "complexity": metric_data["complexity"],
            "metric_expression": metric_data.get("metric_expression"),
            "periods": metric_data.get("periods") or [],
            "grain_aggregation": metric_data.get("grain_aggregation") or "sum",
            "aggregations": metric_data.get("aggregations") or [],
            "owned_by_team": metric_data.get("owned_by_team") or [],
            "meta_data": metric_data["metadata"],
        }
        stmt = insert(Metric).values(metric_id=metric_data["id"], **defaults)
        stmt = stmt.on_conflict_do_update(index_elements=["metric_id"], set_=defaults)
        await session.execute(stmt)
        if i % 10 == 0 or i == len(metrics_data):
            logger.info(f"Processed {i}/{len(metrics_data)} metrics")
    await session.commit()
    logger.info("Metrics upsert completed")

    # Refresh metrics
    metrics = await session.scalars(
        select(Metric).options(
            selectinload(Metric.dimensions),  # type: ignore
            selectinload(Metric.influences),  # type: ignore
            selectinload(Metric.components),  # type: ignore
        )
    )
    metric_id_map = {metric.metric_id: metric for metric in metrics.unique().all()}

    logger.info("Updating metric dimensions...")
    for i, metric_data in enumerate(metrics_data, 1):
        metric = metric_id_map[metric_data["id"]]
        # setup influences
        metric.influences = [metric_id_map[influence_id] for influence_id in metric_data.get("influences", [])]
        # setup dimensions
        metric.dimensions = [dimension_id_map[dim_id] for dim_id in metric_data.get("dimensions", [])]
        # setup components
        metric.components = [metric_id_map[component_id] for component_id in metric_data.get("components", [])]
        session.add(metric)
        if i % 10 == 0 or i == len(metrics_data):
            logger.info(f"Updated dimensions for {i}/{len(metrics_data)} metrics")
    await session.commit()
    logger.info("Metric dimensions update completed")


async def main() -> None:
    """
    Main function to run the upsert process.
    """
    settings = get_settings()
    engine = create_async_engine(settings.DATABASE_URL)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)  # type: ignore
    dimensions_file_path = settings.PATHS.BASE_DIR / "data/dimensions.json"
    metrics_file_path = settings.PATHS.BASE_DIR / "data/metrics.json"
    async with async_session() as session:
        await upsert_data(session, str(dimensions_file_path), str(metrics_file_path))


# Usage example:
if __name__ == "__main__":
    asyncio.run(main())
