import logging
import re
from pathlib import Path
from typing import Any

import pandas as pd
from pydantic import BaseModel, Field

from commons.models.enums import Granularity
from query_manager.config import get_settings
from query_manager.core.enums import Complexity, CubeFilterOperator, MetricAim
from query_manager.core.models import (
    CubeFilter,
    Metric,
    MetricMetadata,
    SemanticMetaMetric,
    SemanticMetaTimeDimension,
)
from query_manager.services.cube import CubeClient

logger = logging.getLogger(__name__)


class CSVColumnConfig:
    """Configuration for standardized CSV column names."""

    # Required columns (system will fail if these are missing)
    REQUIRED_COLUMNS = {
        "measure": "measure",
        "cube": "cube",
        "time_dimension": "time_dimension",
        "dimension_cubes": "dimension_cubes",
    }

    # Optional columns (system will work without these)
    OPTIONAL_COLUMNS = {
        "metric_name": "metric_name",
        "definition": "definition",
        "filters": "filters",
        "owning_team": "owning_team",
        "grain_aggregation": "grain_aggregation",
        "complexity": "complexity",
        "aim": "aim",
        "unit": "unit",
        "unit_of_measure": "unit_of_measure",
        "aggregation_type": "aggregation_type",
    }

    @classmethod
    def get_all_columns(cls) -> dict[str, str]:
        """Get all expected columns (required + optional)."""
        return {**cls.REQUIRED_COLUMNS, **cls.OPTIONAL_COLUMNS}

    @classmethod
    def validate_csv_columns(cls, df: pd.DataFrame) -> tuple[bool, list[str], list[str]]:
        """
        Validate CSV columns against expected schema, handling both old and new column formats.

        Returns:
            tuple: (is_valid, missing_required, available_optional)
        """
        df_columns = set(df.columns)

        # Check required columns
        missing_required = []
        for _, expected_name in cls.REQUIRED_COLUMNS.items():
            if expected_name in df_columns:
                continue
            missing_required.append(expected_name)

        # Check available optional columns
        available_optional = []
        for _, expected_name in cls.OPTIONAL_COLUMNS.items():
            if expected_name in df_columns:
                available_optional.append(expected_name)

        is_valid = len(missing_required) == 0
        return is_valid, missing_required, available_optional


class CSVMetricData(BaseModel):
    """Represents metric data from the standardized CSV file."""

    # Required fields
    measure: str = Field(..., description="Cube measure name")
    cube: str = Field(..., description="Canonical cube name")
    dimension_cubes: list[str] = Field(default_factory=list, description="Related dimension cubes")

    # Optional fields with defaults
    metric_name: str | None = Field(None, description="Display name for the metric")
    definition: str | None = Field(None, description="Metric definition/description")
    filters: str | None = Field(None, description="Cube filters in string format")
    owning_team: str | None = Field(None, description="Team(s) responsible for metric")
    time_dimension: str | None = Field(None, description="Time dimension for this metric")

    # Future extensibility fields
    grain_aggregation: str | None = Field(None, description="Grain aggregation method")
    complexity: str | None = Field(None, description="Metric complexity level")
    aim: str | None = Field(None, description="Metric aim/goal")
    unit: str | None = Field(None, description="Metric unit")
    unit_of_measure: str | None = Field(None, description="Unit of measure")
    aggregation_type: str | None = Field(None, description="Aggregation type")

    @classmethod
    def from_csv_row(cls, row: dict[str, Any]) -> "CSVMetricData":
        """Create CSVMetricData from a CSV row, handling both old and new column formats."""

        # Also include any columns that are already in the new format
        for key, value in row.items():
            if key not in CSVColumnConfig.get_all_columns() and key.lower() in [
                "metric_name",
                "definition",
                "owning_team",
                "filters",
                "time_dimension",
                "cube",
                "measure",
                "dimension_cubes",
                "grain_aggregation",
                "complexity",
                "aim",
                "unit",
                "unit_of_measure",
                "aggregation_type",
            ]:
                row[key] = value

        def safe_get(key: str) -> str | None:
            """Safely get string value from normalized row, handling NaN values."""
            value = row.get(key, "")
            if pd.isna(value) or value == "":
                return None
            return str(value).strip() or None

        def safe_get_required(key: str) -> str:
            """Get required string value, raise error if missing."""
            value = safe_get(key)
            if value is None:
                raise ValueError(f"Required column '{key}' is missing or empty")
            return value

        # Parse dimension cubes from the dimension_cubes column
        dimension_cubes = []
        dimension_cubes_str = safe_get("dimension_cubes")
        if dimension_cubes_str:
            # Split by comma and clean up cube names
            cubes = [cube.strip() for cube in dimension_cubes_str.split(",")]
            dimension_cubes = [cube for cube in cubes if cube]

        return cls(
            # Required fields
            measure=safe_get_required("measure"),
            cube=safe_get_required("cube"),
            dimension_cubes=dimension_cubes,
            # Optional fields
            metric_name=safe_get("metric_name"),
            definition=safe_get("definition"),
            filters=safe_get("filters"),
            owning_team=safe_get("owning_team"),
            time_dimension=safe_get("time_dimension"),
            # Future extensibility fields
            grain_aggregation=safe_get("grain_aggregation"),
            complexity=safe_get("complexity"),
            aim=safe_get("aim"),
            unit=safe_get("unit"),
            unit_of_measure=safe_get("unit_of_measure"),
            aggregation_type=safe_get("aggregation_type"),
        )

    def get_unique_dimension_cubes(self) -> list[str]:
        """Get unique dimension cubes, removing duplicates and base cube suffixes."""
        unique_cubes = []
        cube_groups = set()

        for cube in self.dimension_cubes:
            if cube:
                # Group example_cube and example_cube__base as the same family
                base_cube = cube.replace("__base", "")
                if base_cube not in cube_groups:
                    unique_cubes.append(cube)  # Keep original name
                    cube_groups.add(base_cube)

        return sorted(unique_cubes)

    @classmethod
    def extract_dimension_cubes_from_csv(cls, csv_file_path: str | Path) -> list[str]:
        """
        Extract unique dimension cubes from all rows in a CSV file.

        Args:
            csv_file_path: Path to the CSV file

        Returns:
            List of unique dimension cube names

        Raises:
            ValueError: If CSV doesn't have required columns
        """
        try:
            # Read CSV
            df = pd.read_csv(csv_file_path)
            all_dimension_cubes = set()

            for _, row in df.iterrows():
                csv_data = cls.from_csv_row(row.to_dict())
                all_dimension_cubes.update(csv_data.get_unique_dimension_cubes())

            return sorted(all_dimension_cubes)

        except Exception as e:
            logger.error(f"Error processing CSV file {csv_file_path}: {e}")
            raise

    @classmethod
    def validate_and_load_csv(cls, csv_file_path: str | Path) -> tuple[bool, str, list["CSVMetricData"]]:
        """
        Validate CSV file and load data if valid.

        Returns:
            tuple: (is_valid, error_message, csv_data_list)
        """
        try:
            df = pd.read_csv(csv_file_path)

            # Validate CSV structure
            is_valid, missing_required, available_optional = CSVColumnConfig.validate_csv_columns(df)

            if not is_valid:
                error_msg = f"CSV validation failed. Missing required columns: {missing_required}. "
                error_msg += f"Required columns: {list(CSVColumnConfig.REQUIRED_COLUMNS.values())}. "
                error_msg += f"Available optional columns: {available_optional}. "
                error_msg += "Please update your CSV file with the correct column names."
                return False, error_msg, []

            # Load data
            csv_data_list = []
            for _, row in df.iterrows():
                csv_data = cls.from_csv_row(row.to_dict())
                csv_data_list.append(csv_data)

            success_msg = f"CSV loaded successfully. {len(csv_data_list)} rows processed. "
            success_msg += f"Available optional columns: {available_optional}"

            return True, success_msg, csv_data_list

        except Exception as e:
            return False, f"Error processing CSV file: {str(e)}", []


class CubeMeasure(BaseModel):
    """Represents a cube measure from YAML or API."""

    name: str
    title: str | None = None
    short_title: str | None = None
    description: str | None = None
    type: str = "number"
    format: str | None = None
    sql: str | None = None
    filters: list[dict[str, Any]] = []


class CubeDimension(BaseModel):
    """Represents a cube dimension from YAML or API."""

    name: str
    type: str
    title: str | None = None
    description: str | None = None
    sql: str | None = None


class CubeDefinition(BaseModel):
    """Represents a complete cube definition."""

    name: str
    public: bool = True
    measures: list[CubeMeasure] = []
    dimensions: list[CubeDimension] = []


class CubeMetadataService:
    """Service for generating metrics and dimensions from cube definitions with CSV filtering."""

    def __init__(self):
        self.settings = get_settings()

    async def get_cube_client_for_tenant(self, tenant_id: int):
        """
        Get a CubeClient for a specific tenant using the dependency functions.

        Args:
            tenant_id: The tenant ID to get cube configuration for

        Returns:
            CubeClient instance configured for the tenant

        Raises:
            Exception: If cube configuration is not found for the tenant
        """
        from commons.utilities.context import set_tenant_id
        from query_manager.core.dependencies import get_cube_client, get_insights_backend_client

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

    def validate_csv_file(self, csv_file_path: str | Path) -> tuple[bool, str]:
        """
        Validate CSV file structure before processing.

        Args:
            csv_file_path: Path to the CSV file

        Returns:
            tuple: (is_valid, message)
        """
        try:
            df = pd.read_csv(csv_file_path)
            is_valid, missing_required, available_optional = CSVColumnConfig.validate_csv_columns(df)

            if not is_valid:
                error_msg = "❌ CSV validation failed!\n"
                error_msg += f"Missing required columns: {missing_required}\n"
                error_msg += f"Required columns: {list(CSVColumnConfig.REQUIRED_COLUMNS.values())}\n"
                error_msg += f"Available optional columns: {available_optional}\n"
                error_msg += "Please update your CSV file with the correct column names."
                return False, error_msg

            success_msg = "✅ CSV validation passed!\n"
            success_msg += f"Available optional columns: {available_optional}"
            return True, success_msg

        except Exception as e:
            return False, f"❌ Error reading CSV file: {str(e)}"

    def _prepare_metric_data(self, metric: Metric, include_metric_id: bool = False) -> dict[str, Any]:
        """
        Consolidated method to prepare metric data for both JSON and database.

        Args:
            metric: The Metric object to prepare
            include_metric_id: Whether to include metric_id in the output (for JSON, not DB)
        """
        # Convert periods to strings properly
        periods_str = []
        if metric.periods:
            for p in metric.periods:
                if hasattr(p, "value"):
                    periods_str.append(p.value)
                else:
                    periods_str.append(str(p))

        # Convert meta_data to dict format for database
        meta_data_dict = {
            "semantic_meta": {
                "cube": metric.meta_data.semantic_meta.cube,
                "member": metric.meta_data.semantic_meta.member,
                "member_type": "measure",
                "time_dimension": {
                    "cube": metric.meta_data.semantic_meta.time_dimension.cube,
                    "member": metric.meta_data.semantic_meta.time_dimension.member,
                },
                "cube_filters": (
                    [
                        {"dimension": f.dimension, "operator": f.operator, "values": f.values}
                        for f in metric.meta_data.semantic_meta.cube_filters
                    ]
                    if metric.meta_data.semantic_meta.cube_filters
                    else []
                ),
            }
        }

        data = {
            "label": metric.label,
            "abbreviation": metric.abbreviation,
            "definition": metric.definition,
            "unit": metric.unit,
            "unit_of_measure": metric.unit_of_measure,
            "complexity": metric.complexity,  # Enum is stored as-is for database
            "aim": metric.aim.value if hasattr(metric.aim, "value") else str(metric.aim) if metric.aim else None,  # type: ignore
            "grain_aggregation": metric.grain_aggregation,
            "periods": periods_str,
            "aggregations": metric.aggregations,
            "owned_by_team": metric.owned_by_team,
            "terms": metric.terms if metric.terms else [],
            "metric_expression": None,
            "meta_data": meta_data_dict,
            "hypothetical_max": None,
        }

        # Add metric_id for JSON output but not for database (it's passed separately)
        if include_metric_id:
            data["metric_id"] = metric.metric_id
            # Rename for JSON output consistency and add dimensions for JSON
            data["metadata"] = data.pop("meta_data")
            data["dimensions"] = []
            # Convert complexity enum to string for JSON
            data["complexity"] = (
                metric.complexity.value if hasattr(metric.complexity, "value") else str(metric.complexity)
            )

        return data

    def _parse_csv_file(self, csv_file_path: str | Path) -> dict[str, list[CSVMetricData]]:
        """
        Parse CSV file and create case-insensitive lookup map.

        Args:
            csv_file_path: Path to the CSV file to parse

        Returns:
            Dictionary mapping lowercase measure names to lists of CSVMetricData objects
        """
        try:
            # Read CSV
            df = pd.read_csv(csv_file_path)
            csv_metrics_map: dict = {}
            processed_rows = set()

            for idx, row in df.iterrows():
                try:
                    csv_data = CSVMetricData.from_csv_row(row.to_dict())
                    if csv_data.measure:
                        # Create unique identifier to avoid duplicates
                        row_id = f"{csv_data.measure}_{csv_data.metric_name or ''}_{csv_data.filters or ''}"
                        if row_id in processed_rows:
                            continue
                        processed_rows.add(row_id)

                        # Clean measure name - remove cube prefix if present and make lowercase for matching
                        clean_measure = csv_data.measure.split(".")[-1] if "." in csv_data.measure else csv_data.measure
                        clean_measure_lower = clean_measure.lower().strip()

                        if clean_measure_lower not in csv_metrics_map:
                            csv_metrics_map[clean_measure_lower] = []
                        csv_metrics_map[clean_measure_lower].append(csv_data)

                except Exception as e:
                    logger.warning(f"Error parsing CSV row {idx}: {e}")
                    continue

            unique_measures = len(csv_metrics_map)
            total_variants = sum(len(variants) for variants in csv_metrics_map.values())  # type: ignore
            logger.info(f"Parsed {unique_measures} unique measures with {total_variants} total variants from CSV file")
            return csv_metrics_map

        except Exception as e:
            logger.error(f"Error reading CSV file {csv_file_path}: {e}")
            return {}

    def _generate_metrics_for_cube(
        self, cube_name: str, measures: list[CubeMeasure], csv_file_path: str | Path | None = None
    ) -> list[Metric]:
        """
        Generate metrics for a cube definition.

        Args:
            cube_name: Name of the cube
            measures: List of cube measures
            csv_file_path: Optional path to CSV file for filtering and enhancing metrics

        Returns:
            List of generated Metric objects
        """
        cube = CubeDefinition(name=cube_name, measures=measures, dimensions=[])  # type: ignore

        # Parse CSV data if provided
        csv_metrics_map = {}
        if csv_file_path:
            csv_metrics_map = self._parse_csv_file(csv_file_path)

        metrics = []

        time_dimension = self._find_primary_time_dimension(cube)

        for measure in cube.measures:
            try:
                # Use case-insensitive lookup
                measure_name_lower = measure.name.lower().strip()
                csv_data_list = csv_metrics_map.get(measure_name_lower, [])
                logger.info(
                    f"Processing measure: {measure.name} (lookup: {measure_name_lower}),"
                    f" CSV data found: {len(csv_data_list)} entries"
                )

                if csv_metrics_map and not csv_data_list:
                    # If CSV filtering is enabled and measure is not in CSV, skip with info
                    logger.info(
                        f"Skipping measure {measure.name} - not found in CSV file (looked for: {measure_name_lower})"
                    )
                    continue

                if not csv_data_list:
                    # Create basic metric without CSV data
                    logger.info(f"Creating basic metric for measure {measure.name}")
                    metric = self._create_metric_from_measure(cube.name, measure, time_dimension, None, None)
                    metrics.append(metric)
                else:
                    # Generate metrics for all CSV variants
                    logger.info(f"Creating {len(csv_data_list)} CSV-enhanced metrics for measure {measure.name}")
                    for i, csv_data in enumerate(csv_data_list):
                        metric_id = (
                            measure.name
                            if i == 0
                            else self._generate_variant_metric_id(measure.name, csv_data, csv_data_list[0])
                        )

                        metric = self._create_metric_from_measure(
                            cube.name, measure, time_dimension, csv_data, metric_id
                        )
                        metrics.append(metric)

            except Exception as e:
                logger.error(f"Error generating metric for measure {measure.name}: {str(e)}")
                continue

        return metrics

    async def load_metrics_from_cube(
        self, cube_client: CubeClient, cube_name: str | None = None, csv_file_path: str | Path | None = None
    ) -> list[Metric]:
        """
        Load metrics from cube API. This function does NOT deal with dimensions.

        Args:
            cube_client: CubeClient instance to fetch cube data
            cube_name: Optional name of specific cube. If None, loads from all cubes.
            csv_file_path: Optional path to CSV file for filtering and enhancing metrics

        Returns:
            A list of Metric objects
        """
        # Get measures from cube(s)
        cube_measures = await cube_client.get_cube_measures(cube_name)

        logger.info(f"Processing {len(cube_measures)} measures from cube(s)")

        if not cube_measures:
            logger.warning(f"No measures found for cube: {cube_name}")
            return []

        # Create CubeMeasure objects for all measures
        cube_measures_objects = []
        for measure in cube_measures:
            # Strip cube prefix from measure name for consistency with CSV
            measure_name = measure["name"]
            if "." in measure_name:
                measure_name = measure_name.split(".")[-1]

            cube_measures_objects.append(
                CubeMeasure(
                    name=measure_name,
                    title=measure.get("title"),
                    short_title=measure.get("short_title"),
                    description=measure.get("description"),
                    type=measure.get("type", "number"),
                    format=measure.get("format"),
                    sql=measure.get("sql"),
                    filters=measure.get("filters", []),
                )
            )

        logger.info(f"Created {len(cube_measures_objects)} CubeMeasure objects")

        # Generate metrics for measures from cube and csv file
        metrics = self._generate_metrics_for_cube(cube_name, cube_measures_objects, csv_file_path)  # type: ignore
        logger.info(f"Generated {len(metrics)} metrics from cube(s)")
        return metrics

    async def load_dimensions_from_cube(self, cube_client, cube_name: str | None = None) -> list[dict[str, Any]]:
        """
        Load dimension definitions from cube API.

        Args:
            cube_client: CubeClient instance to fetch cube data
            cube_name: Optional name of specific cube. If None, loads from all cubes.

        Returns:
            A list of dimension dictionaries in the format used by dimensions.json
        """

        dimensions = []
        processed_dimension_ids = set()

        # Get dimensions from cube(s)
        cube_dimensions = await cube_client.get_cube_dimensions(cube_name)

        logger.info(f"Processing {len(cube_dimensions)} dimensions from cube(s)")

        for cube_dim in cube_dimensions:
            try:
                # Strip cube prefix from dimension name for consistency
                dimension_name = cube_dim["name"]
                if "." in dimension_name:
                    dimension_name = dimension_name.split(".")[-1]

                # Create dimension ID
                dim_id = dimension_name.lower().strip()

                # Skip if already processed
                if dim_id in processed_dimension_ids:
                    continue

                # Create dimension dictionary
                dimension = {
                    "id": dim_id,
                    "label": cube_dim.get("title") or cube_dim.get("short_title") or dimension_name,
                    "reference": dim_id,
                    "definition": cube_dim.get(
                        "description", f"Dimension {dimension_name} from cube {cube_dim['cube']}"
                    ),
                    "metadata": {
                        "semantic_meta": {
                            "cube": cube_dim["cube"],
                            "member": dimension_name,  # Use stripped name
                            "member_type": "dimension",
                        }
                    },
                }

                dimensions.append(dimension)
                processed_dimension_ids.add(dim_id)

            except Exception as e:
                logger.error(f"Error processing dimension {cube_dim.get('name', 'unknown')}: {str(e)}")

        logger.info(f"Loaded {len(dimensions)} unique dimensions from cube(s)")
        return dimensions

    async def filter_dimensions_by_value_count(
        self, dimensions: list[dict[str, Any]], cube_client, max_values: int = 15
    ) -> list[dict[str, Any]]:
        """
        Filter dimensions by the number of unique values they have using sequential processing.
        Only returns dimensions with <= max_values unique values.

        Args:
            dimensions: List of dimension dictionaries to filter
            cube_client: CubeClient instance to fetch dimension members
            max_values: Maximum number of unique values to include a dimension

        Returns:
            A list of filtered dimension objects with their values
        """
        from query_manager.core.models import Dimension, DimensionMetadata, SemanticMetaDimension

        filtered_dimensions = []

        logger.info(
            f"🚀 Starting sequential dimension filtering for {len(dimensions)} dimensions (max_values <= {max_values})"
        )

        for i, dim in enumerate(dimensions, 1):
            try:
                # Create a temporary Dimension object to use with the CubeClient
                temp_dimension = Dimension(
                    dimension_id=dim["id"],
                    label=dim["label"],
                    reference=dim["reference"],
                    definition=dim["definition"],
                    meta_data=DimensionMetadata(
                        semantic_meta=SemanticMetaDimension(
                            cube=dim["metadata"]["semantic_meta"]["cube"],
                            member=dim["metadata"]["semantic_meta"]["member"],
                        )
                    ),
                )

                members = await cube_client.load_dimension_members_from_cube(temp_dimension)

                if members and len(members) <= max_values:
                    logger.info(
                        f"✅ Dimension {dim['id']} ({dim['label']}) has {len(members)} values, adding to result"
                    )

                    # Add values to the dimension dictionary
                    result_dim = dim.copy()
                    result_dim["values"] = members
                    result_dim["value_count"] = len(members)
                    filtered_dimensions.append(result_dim)
                else:
                    logger.debug(
                        f"❌ Dimension {dim['id']} ({dim['label']}) has {len(members) if members else 0} values, "
                        f"skipping"
                    )

                # Progress logging
                if i % 5 == 0:
                    logger.info(f"Processed {i}/{len(dimensions)} dimensions...")

            except Exception as e:
                logger.error(f"💥 Error fetching members for dimension {dim['id']}: {str(e)}")

        return filtered_dimensions

    def _create_metric_from_measure(
        self,
        cube_name: str,
        measure: CubeMeasure,
        time_dimension: SemanticMetaTimeDimension,
        csv_data: CSVMetricData | None = None,
        metric_id: str | None = None,
    ) -> Metric:
        """
        Create a metric from cube measure and optional CSV data.

        Args:
            cube_name: Name of the cube
            measure: CubeMeasure object containing measure details
            time_dimension: SemanticMetaTimeDimension object containing time dimension details
            csv_data: Optional CSVMetricData object containing CSV data
            metric_id: Optional metric ID to use

        Returns:
            Generated Metric object
        """
        # Create semantic metadata
        semantic_meta = self._create_semantic_meta(cube_name, measure, time_dimension, csv_data)

        # Determine metric properties - use just the measure name for metric_id
        metric_id = metric_id or measure.name
        label = (
            csv_data.metric_name
            if csv_data and csv_data.metric_name
            else (measure.short_title or measure.title or measure.name)
        )
        definition = csv_data.definition if csv_data and csv_data.definition else measure.description

        # Parse owned_by_team from CSV
        owned_by_team = []
        if csv_data and csv_data.owning_team:
            owned_by_team = [team.strip() for team in csv_data.owning_team.split(",") if team.strip()]

        # Use CSV values if available, otherwise fall back to measure-based logic
        unit_of_measure = (
            csv_data.unit_of_measure if csv_data and csv_data.unit_of_measure else self._get_unit_of_measure(measure)
        )

        unit = csv_data.unit if csv_data and csv_data.unit else self._get_unit(measure)

        complexity = self._get_complexity(measure)

        aim = self._get_aim(measure)

        grain_aggregation = self._get_aggregation_type(measure.type)

        aggregation_type = self._get_aggregation_type(measure.type)

        # Create metric object with CSV data if available, otherwise use measure-based logic
        return Metric(
            metric_id=metric_id,
            label=label,
            abbreviation=metric_id,
            definition=definition,
            unit_of_measure=unit_of_measure,
            unit=unit,
            terms=[],
            complexity=complexity,
            metric_expression=None,
            periods=[Granularity.DAY, Granularity.WEEK, Granularity.MONTH],
            grain_aggregation=grain_aggregation,
            aggregations=[aggregation_type],
            owned_by_team=owned_by_team,
            meta_data=MetricMetadata(semantic_meta=semantic_meta),
            dimensions=[],
            aim=aim,
        )

    def _create_semantic_meta(
        self,
        cube_name: str,
        measure: CubeMeasure,
        time_dimension: SemanticMetaTimeDimension,
        csv_data: CSVMetricData | None = None,
    ) -> SemanticMetaMetric:
        """Create semantic metadata for a metric.

        Args:
            cube_name: Name of the cube
            measure: CubeMeasure object containing measure details
            time_dimension: SemanticMetaTimeDimension object containing time dimension details (fallback)
            csv_data: Optional CSVMetricData object containing CSV data
        """
        # Use time_dimension from CSV if available, otherwise use the default
        final_time_dimension = time_dimension
        if csv_data and csv_data.time_dimension:
            final_time_dimension = SemanticMetaTimeDimension(cube=cube_name, member=csv_data.time_dimension)

        # Parse filters from CSV or measure
        cube_filters = []
        if csv_data and csv_data.filters:
            cube_filters = self._parse_csv_filters(csv_data.filters, cube_name)
        elif measure.filters:
            cube_filters = self._parse_measure_filters(measure.filters, cube_name)

        return SemanticMetaMetric(
            cube=cube_name, member=measure.name, time_dimension=final_time_dimension, cube_filters=cube_filters
        )

    def _parse_csv_filters(self, filters_str: str, cube_name: str) -> list[CubeFilter]:
        """Parse CSV filter string into CubeFilter objects.

        Args:
            filters_str: String containing filter conditions
            cube_name: Name of the cube

        Returns:
            List of parsed CubeFilter objects
        """

        cube_filters = []
        filter_parts = [f.strip() for f in filters_str.split(" AND ")]

        for filter_part in filter_parts:
            if not filter_part or filter_part.lower() == "none":
                continue

            # Handle equals filters: {dimension} = value
            equals_match = re.match(r"\{(\w+)\}\s*=\s*(.+)", filter_part)
            if equals_match:
                dimension_name, value = equals_match.groups()
                value = value.strip().strip("'\"")

                # Convert TRUE/FALSE to boolean
                if value.upper() in ["TRUE", "FALSE"]:
                    value = value.upper() == "TRUE"

                cube_filters.append(
                    CubeFilter(
                        dimension=f"{cube_name}.{dimension_name}", operator=CubeFilterOperator.EQUALS, values=[value]
                    )
                )
                continue

            # Handle IS NOT NULL filters
            not_null_match = re.match(r"\{(\w+)\}\s+IS\s+NOT\s+NULL", filter_part)
            if not_null_match:
                dimension_name = not_null_match.group(1)
                cube_filters.append(
                    CubeFilter(dimension=f"{cube_name}.{dimension_name}", operator=CubeFilterOperator.SET, values=[])
                )
                continue

            logger.warning(f"Could not parse filter: {filter_part}")

        return cube_filters

    def _parse_measure_filters(self, filters: list[dict[str, Any]], cube_name: str) -> list[CubeFilter]:
        """Parse measure filters into CubeFilter objects.

        Args:
            filters: List of filter dictionaries
            cube_name: Name of the cube

        Returns:
            List of parsed CubeFilter objects
        """

        cube_filters = []
        for filter_dict in filters:
            sql_filter = filter_dict.get("sql", "")
            if not sql_filter:
                continue

            # Handle boolean filters: {dimension} = true/false
            bool_match = re.match(r"\{(\w+)\}\s*=\s*(true|false)", sql_filter)
            if bool_match:
                dimension_name, value = bool_match.groups()
                cube_filters.append(
                    CubeFilter(
                        dimension=f"{cube_name}.{dimension_name}",
                        operator=CubeFilterOperator.EQUALS,
                        values=[value.lower() == "true"],
                    )
                )
                continue

            # Handle NOT NULL filters
            not_null_match = re.match(r"\{(\w+)\}\s+IS\s+NOT\s+NULL", sql_filter)
            if not_null_match:
                dimension_name = not_null_match.group(1)
                cube_filters.append(
                    CubeFilter(dimension=f"{cube_name}.{dimension_name}", operator=CubeFilterOperator.SET, values=[])
                )
                continue

        return cube_filters

    def _find_primary_time_dimension(self, cube: CubeDefinition) -> SemanticMetaTimeDimension:
        """Find primary time dimension for a cube.

        Args:
            cube: CubeDefinition object containing cube measures and dimensions

        Returns:
            SemanticMetaTimeDimension object containing time dimension details
        """

        for dimension in cube.dimensions:
            if dimension.type == "time":
                return SemanticMetaTimeDimension(cube=cube.name, member=dimension.name)

        # Default fallback
        return SemanticMetaTimeDimension(cube=cube.name, member="created_at")

    def _generate_variant_metric_id(
        self, base_measure: str, csv_data: CSVMetricData, base_csv_data: CSVMetricData
    ) -> str:
        """Generate unique metric ID for variant metrics.

        Args:
            base_measure: Base measure name
            csv_data: CSVMetricData object containing CSV data
            base_csv_data: Base CSVMetricData object containing base CSV data

        Returns:
            Generated metric ID
        """
        keywords = []

        # Extract keywords from filters
        if csv_data.filters:
            keywords.extend(self._extract_filter_keywords(csv_data.filters))

        # Extract from metric name differences if no filter keywords
        if not keywords and csv_data.metric_name and base_csv_data.metric_name:
            base_name = base_csv_data.metric_name.lower()
            variant_name = csv_data.metric_name.lower()

            if "," in variant_name:
                variant_part = variant_name.split(",")[-1].strip()
                keywords.extend(variant_part.split())
            else:
                variant_parts = variant_name.replace(base_name, "").split()
                keywords.extend(
                    [part.strip(",") for part in variant_parts if part.strip(",") and len(part.strip(",")) > 1]
                )

        # Clean and format keywords
        clean_keywords = [
            keyword.lower().replace("-", "_").replace(" ", "_")
            for keyword in keywords
            if keyword.lower()
            not in [
                "the",
                "a",
                "an",
                "and",
                "or",
                "but",
                "in",
                "on",
                "at",
                "to",
                "for",
                "of",
                "with",
                "by",
                "sourced",
                "new",
                "rate",
            ]
            and len(keyword) > 1
        ]

        # Remove duplicates while preserving order
        unique_keywords = []
        seen = set()
        for keyword in clean_keywords:
            if keyword not in seen:
                unique_keywords.append(keyword)
                seen.add(keyword)

        # Create suffix
        suffix = (
            "_".join(unique_keywords[:2]) if unique_keywords else f"variant_{len(csv_data.metric_name or '') % 100:02d}"
        )

        return f"{base_measure}_{suffix}"

    def _extract_filter_keywords(self, filters_str: str) -> list[str]:
        """Extract keywords from filter strings.

        Args:
            filters_str: String containing filter conditions

        Returns:
            List of extracted keywords
        """
        keywords = []
        filters_lower = filters_str.lower().strip()

        # Remove SQL syntax
        filters_lower = (
            filters_lower.replace("{", "").replace("}", "").replace("=", " ").replace("'", "").replace('"', "")
        )

        # Extract meaningful keywords
        keyword_patterns = [
            ("event qualified", ["event"]),
            ("inbound request", ["inbound"]),
            ("ae outbound", ["ae_outbound"]),
            ("sdr outbound", ["sdr_outbound"]),
            ("partner-sourced", ["partner"]),
            ("direct-to-pro", ["direct_pro"]),
            ("event", ["event"]),
            ("inbound", ["inbound"]),
            ("enterprise", ["ent"]),
            ("renewal", ["renewal"]),
            ("expansion", ["expansion"]),
        ]

        for pattern, pattern_keywords in keyword_patterns:
            if pattern in filters_lower:
                keywords.extend(pattern_keywords)
                filters_lower = filters_lower.replace(pattern, "")

        return list(dict.fromkeys(keywords))  # Remove duplicates while preserving order

    # Utility methods - these can remain static since they're pure functions
    @staticmethod
    def _get_unit_of_measure(measure: CubeMeasure) -> str:
        """Get unit of measure from measure.

        Args:
            measure: CubeMeasure object containing measure details

        Returns:
            String representing the unit of measure
        """
        format_type = measure.format
        title = (measure.title or measure.short_title or "").lower()

        if format_type == "percent" or "%" in title:
            return "Percent"
        elif "time" in title or "days" in title:
            return "Time"
        elif "$" in title or "revenue" in title or "cost" in title:
            return "Currency"
        else:
            return "Quantity"

    @staticmethod
    def _get_unit(measure: CubeMeasure) -> str:
        """Get unit from measure.

        Args:
            measure: CubeMeasure object containing measure details

        Returns:
            String representing the unit
        """
        format_type = measure.format
        title = (measure.title or measure.short_title or "").lower()

        if format_type == "percent" or "%" in title:
            return "%"
        elif "time" in title or "days" in title:
            return "days"
        elif "$" in title or "revenue" in title or "cost" in title:
            return "$"
        else:
            return "n"

    @staticmethod
    def _get_complexity(measure: CubeMeasure) -> Complexity:
        """Get complexity from measure.

        Args:
            measure: CubeMeasure object containing measure details

        Returns:
            Complexity enum value
        """
        sql = measure.sql or ""
        # If SQL contains references to other measures, it's complex
        if "{" in sql and "}" in sql and measure.type == "number":
            return Complexity.COMPLEX
        return Complexity.ATOMIC

    @staticmethod
    def _get_aim(measure: CubeMeasure) -> MetricAim:
        """Get aim from measure.

        Args:
            measure: CubeMeasure object containing measure details

        Returns:
            MetricAim enum value
        """
        title = (measure.title or measure.short_title or "").lower()
        minimize_keywords = ["time", "days", "cost", "churn", "loss", "error", "disqualified"]

        if any(keyword in title for keyword in minimize_keywords):
            return MetricAim.MINIMIZE
        return MetricAim.MAXIMIZE

    @staticmethod
    def _get_aggregation_type(cube_type: str) -> str:
        """Get aggregation type from cube type.

        Args:
            cube_type: String representing the cube type

        Returns:
            String representing the aggregation type
        """
        aggregation_map = {
            "count": "sum",
            "count_distinct": "sum",
            "sum": "sum",
            "avg": "avg",
            "min": "min",
            "max": "max",
            "number": "sum",
        }
        return aggregation_map.get(cube_type, "sum")
