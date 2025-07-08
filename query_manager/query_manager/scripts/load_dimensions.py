import asyncio
import json
import logging
from pathlib import Path
from typing import Any

import yaml
from sqlalchemy.dialects.postgresql import insert

from commons.utilities.context import reset_context, set_tenant_id
from commons.utilities.tenant_utils import validate_tenant
from query_manager.config import get_settings
from query_manager.core.dependencies import get_cube_client, get_insights_backend_client
from query_manager.core.models import Dimension, DimensionMetadata, SemanticMetaDimension
from query_manager.db.config import get_async_session
from query_manager.services.cube import CubeClient

logger = logging.getLogger(__name__)


async def get_cube_client_for_tenant(tenant_id: int) -> CubeClient:
    """
    Get a CubeClient for a specific tenant using the dependency functions.

    Args:
        tenant_id: The tenant ID to get cube configuration for

    Returns:
        CubeClient instance configured for the tenant

    Raises:
        Exception: If cube configuration is not found for the tenant
    """
    # Set tenant context
    set_tenant_id(tenant_id)

    try:
        # Use the same dependency functions as the web API
        insights_backend_client = await get_insights_backend_client()
        cube_client = await get_cube_client(insights_backend_client)
        return cube_client

    except Exception as e:
        logger.error(f"Error getting cube client for tenant {tenant_id}: {str(e)}")
        raise


async def load_dimensions_from_yaml_files(yaml_file: Path) -> list[dict[str, Any]]:
    """
    Load dimension definitions from YAML file.

    Args:
        yaml_file: Path to a YAML file

    Returns:
        A list of dimension dictionaries in the format used by dimensions.json
    """
    dimensions = []
    processed_dimension_ids = set()
    logger.info(f"Processing single YAML file: {yaml_file}")

    try:
        logger.info(f"Processing YAML file: {yaml_file}")
        with open(yaml_file) as f:
            yaml_data = yaml.safe_load(f)

        # Extract dimensions from the cubes in the YAML file
        file_dimensions = extract_dimensions_from_yaml(yaml_data)

        # Add dimensions that haven't been processed yet
        for dimension in file_dimensions:
            dim_id = dimension["id"]
            if dim_id not in processed_dimension_ids:
                dimensions.append(dimension)
                processed_dimension_ids.add(dim_id)

    except Exception as e:
        logger.error(f"Error processing YAML file {yaml_file}: {str(e)}")

    logger.info(f"Loaded {len(dimensions)} unique dimensions from {yaml_file}")
    return dimensions


def extract_dimensions_from_yaml(yaml_data: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Extract dimensions from YAML data.

    Args:
        yaml_data: Dictionary containing YAML data

    Returns:
        A list of dimension dictionaries
    """
    dimensions = []

    # Get cubes from the YAML data
    cubes = yaml_data.get("cubes", [])

    for cube in cubes:
        cube_name = cube.get("name")
        if not cube_name:
            continue

        # Process dimensions in the cube
        for dim in cube.get("dimensions", []):
            dim_name = dim.get("name")
            if not dim_name:
                continue

            # Create dimension ID (similar to how metrics are generated)
            dim_id = dim_name.lower().strip()

            # Create dimension dictionary directly
            dimension = {
                "id": dim_id,
                "label": dim.get("title") or dim.get("shortTitle") or dim_name,
                "reference": dim_id,
                "definition": dim.get("description", f"Dimension {dim_name} from cube {cube_name}"),
                "metadata": {"semantic_meta": {"cube": cube_name, "member": dim_name, "member_type": "dimension"}},
            }

            dimensions.append(dimension)

    return dimensions


async def filter_dimensions_by_value_count(
    dimensions: list[dict[str, Any]], tenant_id: int, max_values: int = 15
) -> list[dict[str, Any]]:
    """
    Filter dimensions by the number of unique values they have.
    Only returns dimensions with fewer than max_values unique values.

    Args:
        dimensions: List of dimension dictionaries to filter
        max_values: Maximum number of unique values to include a dimension
        tenant_id: Tenant ID to get cube configuration for (mandatory)

    Returns:
        A list of filtered dimension objects with their values
    """

    cube_client = await get_cube_client_for_tenant(tenant_id)
    filtered_dimensions = []

    for dim in dimensions:
        try:
            # Create a temporary Dimension object to use with the CubeClient
            # This is necessary because cube_client.load_dimension_members_from_cube expects a Dimension object
            temp_dimension = Dimension(
                dimension_id=dim["id"],
                label=dim["label"],
                reference=dim["reference"],
                definition=dim["definition"],
                meta_data=DimensionMetadata(
                    semantic_meta=SemanticMetaDimension(
                        cube=dim["metadata"]["semantic_meta"]["cube"], member=dim["metadata"]["semantic_meta"]["member"]
                    )
                ),
            )

            members = await cube_client.load_dimension_members_from_cube(temp_dimension)

            if members and len(members) <= max_values:
                logger.info(f"Dimension {dim['id']} ({dim['label']}) has {len(members)} values, adding to result")

                # Add values to the dimension dictionary
                result_dim = dim.copy()
                result_dim["values"] = members
                result_dim["value_count"] = len(members)

                filtered_dimensions.append(result_dim)
            else:
                logger.debug(
                    f"Dimension {dim['id']} ({dim['label']}) has {len(members) if members else 0} values, skipping"
                )
        except Exception as e:
            logger.error(f"Error fetching members for dimension {dim['id']}: {str(e)}")

    logger.info(f"Found {len(filtered_dimensions)} dimensions with <= {max_values} values")
    return filtered_dimensions


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


async def save_dimensions_to_db(dimensions: list[dict[str, Any]], tenant_id: int) -> int:
    """
    Save filtered dimensions to the database for a specific tenant.
    Following the pattern from metadata_upsert.py and metric_generator.py

    Args:
        dimensions: List of filtered dimension dictionaries
        tenant_id: ID of the tenant to save dimensions for

    Returns:
        Number of dimensions saved
    """
    settings = get_settings()

    # Set tenant context following metadata_upsert.py pattern
    logger.info(f"Setting tenant context, Tenant ID: {tenant_id}")
    set_tenant_id(tenant_id)

    # Validate tenant following existing pattern
    logger.info(f"Validating Tenant ID: {tenant_id}")
    await validate_tenant(settings, tenant_id)

    saved_count = 0

    try:
        async with get_async_session() as session:
            logger.info(f"Saving {len(dimensions)} dimensions to database for tenant {tenant_id}...")

            for i, dim in enumerate(dimensions, 1):
                try:
                    # Prepare dimension data following metadata_upsert.py pattern
                    defaults = {
                        "label": dim["label"],
                        "reference": dim.get("reference"),
                        "definition": dim.get("definition"),
                        "meta_data": dim["metadata"],
                    }

                    # Use established upsert pattern following metadata_upsert.py
                    stmt = insert(Dimension).values(dimension_id=dim["id"], tenant_id=tenant_id, **defaults)
                    stmt = stmt.on_conflict_do_update(index_elements=["dimension_id", "tenant_id"], set_=defaults)

                    await session.execute(stmt)
                    saved_count += 1

                    # Log progress following existing pattern
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
        # Clean up context following existing pattern
        reset_context()

    return saved_count


async def main(
    tenant_id: int,
    file_path: Path,
    max_values: int = 15,
    output: str | None = None,
    save_to_db: bool = False,
) -> list[dict[str, Any]]:
    """
    This function loads dimensions from a YAML file and filters them by value count.
    It can also save the filtered dimensions to a JSON file or to the database.

    Args:
        tenant_id: Tenant ID to get cube configuration and save dimensions (mandatory)
        file_path: Path to a YAML file
        max_values: Maximum number of unique values to include a dimension
        output: Optional path to save the filtered dimensions as JSON
        save_to_db: Whether to save the filtered dimensions to the database

    Returns:
        A list of filtered dimension objects with their values
    """

    # Load dimensions from YAML files
    dimensions = await load_dimensions_from_yaml_files(file_path)

    # Filter dimensions by value count
    filtered_dimensions = await filter_dimensions_by_value_count(dimensions, tenant_id, max_values)

    # Save to JSON if output path provided
    if output:
        await save_dimensions_to_json(filtered_dimensions, output)

    # Save to DB if save_to_db is True
    if save_to_db:
        saved_count = await save_dimensions_to_db(filtered_dimensions, tenant_id)
        logger.info(f"Saved {saved_count} dimensions to database for tenant {tenant_id}")

    return filtered_dimensions


if __name__ == "__main__":
    # For testing purposes
    import sys

    tenant_id_arg = int(sys.argv[1]) if len(sys.argv) > 1 else None
    file_path = Path(sys.argv[2])
    max_values = int(sys.argv[3]) if len(sys.argv) > 3 else 15
    output_path = sys.argv[4] if len(sys.argv) > 4 else None
    save_to_db_arg = sys.argv[5].lower() == "true" if len(sys.argv) > 5 else False

    if tenant_id_arg is None:
        raise ValueError("tenant_id is required")

    asyncio.run(main(tenant_id_arg, file_path, max_values, output_path, save_to_db_arg))
