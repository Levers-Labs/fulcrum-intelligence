import json
import logging
from pathlib import Path
from typing import Any

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from commons.db.v2 import async_session
from commons.utilities.context import reset_context, set_tenant_id
from commons.utilities.json_utils import serialize_json
from commons.utilities.tenant_utils import validate_tenant
from query_manager.config import get_settings
from query_manager.core.models import Dimension, Metric
from query_manager.services.cube_metadata_service import CSVMetricData, CubeMetadataService

logger = logging.getLogger(__name__)


async def save_dimensions_to_json(dimensions: list[dict[str, Any]], output_path: str | Path) -> None:
    """
    Save dimensions to a JSON file.

    Args:
        dimensions: List of dimension dictionaries
        output_path: Path to save the JSON file
    """
    # Remove the values and value_count fields for saving
    json_format = []
    for dim in dimensions:
        json_dim = {k: v for k, v in dim.items() if k not in ["values", "value_count"]}
        json_format.append(json_dim)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(json_format, f, indent=2)

    logger.info(f"Saved {len(dimensions)} dimensions to: {output_path}")


async def save_metrics_to_json(metrics: list[Metric], output_path: str | Path) -> None:
    """
    Save metrics to a JSON file (without dimensions).

    Args:
        metrics: List of Metric objects
        output_path: Path to save the JSON file
    """
    # Prepare metrics data without dimensions
    metrics_data = []
    metric_generator_service = CubeMetadataService()

    for metric in metrics:
        # Get the base metric data
        metric_data = metric_generator_service._prepare_metric_data(metric, include_metric_id=True)
        metric_data["dimensions"] = []
        metrics_data.append(metric_data)

    serialized_data = serialize_json(metrics_data)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(serialized_data, f, indent=2)

    logger.info(f"Saved {len(metrics)} metrics to: {output_path}")


async def save_metrics_to_json_with_dimensions(
    metrics: list[Metric], metrics_dimension_mapping: dict[str, list[str]], output_path: str | Path
) -> None:
    """
    Save metrics to a JSON file with dimensions included using the dimension mapping.

    Args:
        metrics: List of Metric objects
        metrics_dimension_mapping: Dictionary mapping metric_id to list of dimension_ids
        output_path: Path to save the JSON file
    """

    # Prepare metrics data with dimensions
    metrics_data = []
    metric_generator_service = CubeMetadataService()

    for metric in metrics:
        # Get the base metric data
        metric_data = metric_generator_service._prepare_metric_data(metric, include_metric_id=True)

        # Add dimensions from mapping
        dimension_ids = metrics_dimension_mapping.get(metric.metric_id, [])
        metric_data["dimensions"] = dimension_ids

        metrics_data.append(metric_data)

    serialized_data = serialize_json(metrics_data)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(serialized_data, f, indent=2)

    logger.info(f"Saved {len(metrics)} metrics with dimensions to: {output_path}")


async def save_metrics_to_db(session: AsyncSession, metrics: list[Metric], tenant_id: int) -> int:
    """
    Save metrics to database using established patterns.

    Args:
        metrics: List of Metric objects to save
        tenant_id: ID of the tenant to save metrics for

    Returns:
        Number of metrics saved
    """
    settings = get_settings()

    # Set tenant context following metadata_upsert.py pattern
    logger.info(f"Setting tenant context, Tenant ID: {tenant_id}")
    set_tenant_id(tenant_id)

    # Validate tenant following existing pattern
    logger.info(f"Validating Tenant ID: {tenant_id}")
    await validate_tenant(settings, tenant_id)

    saved_count = 0
    cube_service = CubeMetadataService()

    logger.info(f"Saving {len(metrics)} metrics to database for tenant {tenant_id}...")

    for i, metric in enumerate(metrics, 1):
        try:
            # Use consolidated data preparation from service
            defaults = cube_service._prepare_metric_data(metric, include_metric_id=False)

            # Use established upsert pattern
            stmt = insert(Metric).values(metric_id=metric.metric_id, tenant_id=tenant_id, **defaults)
            stmt = stmt.on_conflict_do_update(index_elements=["metric_id", "tenant_id"], set_=defaults)
            await session.execute(stmt)
            saved_count += 1

            # Log progress following existing pattern
            if i % 10 == 0 or i == len(metrics):
                logger.info(f"Processed {i}/{len(metrics)} metrics")

        except Exception as e:
            logger.error(f"Error saving metric {metric.metric_id}: {str(e)}")
            continue

    await session.commit()
    logger.info(f"Successfully saved {saved_count}/{len(metrics)} metrics to database")

    # Clean up context following existing pattern
    reset_context()

    return saved_count


async def save_dimensions_to_db(session: AsyncSession, dimensions: list[dict[str, Any]], tenant_id: int) -> int:
    """
    Save filtered dimensions to the database for a specific tenant.

    Args:
        dimensions: List of filtered dimension dictionaries
        tenant_id: ID of the tenant to save dimensions for

    Returns:
        Number of dimensions saved
    """
    settings = get_settings()

    # Set tenant context
    logger.info(f"Setting tenant context, Tenant ID: {tenant_id}")
    set_tenant_id(tenant_id)

    # Validate tenant
    logger.info(f"Validating Tenant ID: {tenant_id}")
    await validate_tenant(settings, tenant_id)

    saved_count = 0

    try:
        logger.info(f"Saving {len(dimensions)} dimensions to database for tenant {tenant_id}...")

        for i, dim in enumerate(dimensions, 1):
            try:
                # Prepare dimension data
                defaults = {
                    "label": dim["label"],
                    "reference": dim.get("reference"),
                    "definition": dim.get("definition"),
                    "meta_data": dim["metadata"],
                }

                # Use upsert pattern
                stmt = insert(Dimension).values(dimension_id=dim["id"], tenant_id=tenant_id, **defaults)
                stmt = stmt.on_conflict_do_update(index_elements=["dimension_id", "tenant_id"], set_=defaults)

                await session.execute(stmt)
                saved_count += 1

                # Log progress
                if i % 10 == 0 or i == len(dimensions):
                    logger.info(f"Processed {i}/{len(dimensions)} dimensions")

            except Exception as e:
                logger.error(f"Error saving dimension {dim['id']}: {str(e)}")
                continue

        await session.commit()
        logger.info(f"Successfully saved {saved_count}/{len(dimensions)} dimensions to database")

    except Exception as e:
        logger.error(f"Error saving dimensions to database: {str(e)}")
        raise
    finally:
        # Clean up context
        reset_context()

    return saved_count


async def load_metrics_main(
    session: AsyncSession,
    tenant_id: int,
    cube_name: str | None = None,
    csv_file_path: str | Path | None = None,
    output: str | None = None,
    save_to_db: bool = False,
) -> list[Metric]:
    """
    Main function to load metrics from cube.

    Args:
        tenant_id: Tenant ID to get cube configuration and save metrics (mandatory)
        cube_name: Optional name of specific cube. If None, loads from all cubes.
        csv_file_path: Optional path to CSV file for filtering and enhancing metrics
        output: Optional path to save the metrics as JSON
        save_to_db: Whether to save the metrics to the database

    Returns:
        A list of Metric objects
    """
    # Initialize service and get cube client
    cube_service = CubeMetadataService()
    cube_client = await cube_service.get_cube_client_for_tenant(tenant_id)

    # Validate CSV file if provided
    if csv_file_path:
        is_valid, validation_message = cube_service.validate_csv_file(csv_file_path)
        if not is_valid:
            logger.error(f"CSV validation failed: {validation_message}")
            raise ValueError(f"CSV validation failed: {validation_message}")

    # Load metrics from cube
    metrics = await cube_service.load_metrics_from_cube(cube_client, cube_name, csv_file_path)

    # Save to JSON if output path provided
    if output:
        await save_metrics_to_json(metrics, output)

    # Save to DB if save_to_db is True
    if save_to_db:
        saved_count = await save_metrics_to_db(session, metrics, tenant_id)
        logger.info(f"Saved {saved_count} metrics to database for tenant {tenant_id}")

    return metrics


async def load_metrics_with_dimensions_main(
    session: AsyncSession,
    tenant_id: int,
    csv_file_path: str | Path,
    cube_name: str | None = None,
    max_values: int = 15,
    metrics_output: str | None = None,
    dimensions_output: str | None = None,
    save_to_db: bool = False,
) -> tuple[list[Metric], list[dict[str, Any]]]:
    """
    Load metrics with dimensions using the correct flow:
    1. Get dimensions first (from cube-name OR from CSV dimension cubes if no cube-name)
    2. Filter dimensions by max_values → get list of valid dimension_ids
    3. Load metrics from cube
    4. Attach those dimension_ids to metrics

    Args:
        tenant_id: Tenant ID to get cube configuration and save data (mandatory)
        csv_file_path: Path to CSV file containing metrics and dimension cubes
        cube_name: Optional cube name to process metrics from. If None, loads metrics from all cubes.
        max_values: Maximum number of unique values to include a dimension
        metrics_output: Optional path to save metrics as JSON
        dimensions_output: Optional path to save dimensions as JSON
        save_to_db: Whether to save the data to the database

    Returns:
        A tuple of (metrics, dimensions)
    """

    # Initialize service and get cube client
    cube_service = CubeMetadataService()
    cube_client = await cube_service.get_cube_client_for_tenant(tenant_id)

    # Validate CSV file (required for this command)
    is_valid, validation_message = cube_service.validate_csv_file(csv_file_path)
    if not is_valid:
        logger.error(f"CSV validation failed: {validation_message}")
        raise ValueError(f"CSV validation failed: {validation_message}")

    # Step 1: Determine which cubes to get dimensions from using CSVMetricData
    if cube_name:
        # If cube-name provided, get dimensions from that cube only
        dimension_cubes = [cube_name]
        logger.info(f"Getting dimensions from specified cube: {cube_name}")
    else:
        # If no cube-name, get dimensions from all cubes listed in CSV dimension column
        dimension_cubes = CSVMetricData.extract_dimension_cubes_from_csv(csv_file_path)
        logger.info(f"Getting dimensions from CSV dimension cubes: {dimension_cubes}")

    # Step 2: Load ALL dimensions from the dimension cubes and filter by max_values
    logger.info(f"Loading and filtering dimensions from {len(dimension_cubes)} cubes...")
    all_dimensions = []

    for dimension_cube in dimension_cubes:
        logger.info(f"Loading dimensions from cube: {dimension_cube}")
        cube_dimensions = await cube_service.load_dimensions_from_cube(cube_client, dimension_cube)
        all_dimensions.extend(cube_dimensions)

    # Remove duplicates based on dimension ID using dict comprehension
    unique_dimensions = list({dim["id"]: dim for dim in all_dimensions}.values())

    logger.info(f"Found {len(unique_dimensions)} unique dimensions total")

    # Filter dimensions by value count to get the valid ones
    filtered_dimensions = await cube_service.filter_dimensions_by_value_count(
        unique_dimensions, cube_client, max_values
    )
    logger.info(f"Filtered to {len(filtered_dimensions)} dimensions with <= {max_values} values")

    # Create list of valid dimension IDs for attaching to metrics
    valid_dimension_ids = [dim["id"] for dim in filtered_dimensions]
    logger.info(f"Valid dimension IDs: {valid_dimension_ids}")

    # Step 3: Load metrics from cube
    logger.info("Loading metrics from cube...")
    metrics = await cube_service.load_metrics_from_cube(cube_client, cube_name, csv_file_path)
    logger.info(f"Loaded {len(metrics)} metrics")

    # Step 4: Attach the valid dimension_ids to ALL metrics
    logger.info(f"Attaching {len(valid_dimension_ids)} dimension IDs to all metrics...")
    metrics_dimension_mapping = {}

    for metric in metrics:
        # For this implementation, attach ALL valid dimension_ids to each metric
        # (since they come from the same cube or related cubes)
        metrics_dimension_mapping[metric.metric_id] = valid_dimension_ids

    logger.info(f"Attached dimensions to {len(metrics)} metrics")

    # Save to JSON files if paths provided
    if metrics_output:
        await save_metrics_to_json_with_dimensions(metrics, metrics_dimension_mapping, metrics_output)

    if dimensions_output:
        await save_dimensions_to_json(filtered_dimensions, dimensions_output)

    # Save to DB if save_to_db is True
    if save_to_db:
        # Save dimensions first
        await save_dimensions_to_db(session, filtered_dimensions, tenant_id)
        # Save metrics
        await save_metrics_to_db(session, metrics, tenant_id)

    return metrics, filtered_dimensions


# Example usage with session context manager
async def main(tenant_id: int):
    """
    Example usage of the cube metadata functions with async_session.
    This demonstrates how to call these functions from external code or scripts.
    """

    async with async_session(get_settings(), app_name="query_cube_metadata_loader") as session:
        # Example: Load metrics only
        metrics = await load_metrics_main(
            session=session, tenant_id=tenant_id, cube_name="your_cube_name", output="metrics.json", save_to_db=True
        )

        # Example: Load metrics with dimensions
        metrics, dimensions = await load_metrics_with_dimensions_main(
            session=session,
            tenant_id=tenant_id,
            csv_file_path="path/to/metrics.csv",
            cube_name="your_cube_name",
            max_values=15,
            metrics_output="metrics_with_dims.json",
            dimensions_output="dimensions.json",
            save_to_db=True,
        )


if __name__ == "__main__":
    import argparse
    import asyncio

    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant_id", type=int, required=True)
    args = parser.parse_args()
    asyncio.run(main(args.tenant_id))
