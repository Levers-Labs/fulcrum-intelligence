import json
import logging
import re
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from pydantic import BaseModel
from sqlalchemy.dialects.postgresql import insert

from commons.models.enums import Granularity
from commons.utilities.context import reset_context, set_tenant_id
from commons.utilities.json_utils import serialize_json
from commons.utilities.tenant_utils import validate_tenant
from query_manager.config import get_settings
from query_manager.core.enums import Complexity, MetricAim
from query_manager.core.models import (
    CubeFilter,
    Metric,
    Metric as MetricModel,
    MetricMetadata,
    SemanticMetaMetric,
    SemanticMetaTimeDimension,
)
from query_manager.db.config import get_async_session

logger = logging.getLogger(__name__)


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


class CSVMetricData(BaseModel):
    """Represents metric data from the GTM CSV file."""

    measure: str  # Maps to "Measure" column
    metric_name: str | None = None  # Maps to "Metric Name" column
    definition: str | None = None  # Maps to "Definition" column
    cube: str | None = None  # Maps to "Canonical Cube" column
    anchor_date: str | None = None  # Maps to "Anchor Date" column
    filters: str | None = None  # Maps to "Filters" column
    owning_team: str | None = None  # Maps to "Owning Team(s)" column

    @classmethod
    def from_csv_row(cls, row: dict[str, Any]) -> "CSVMetricData":
        """Create CSVMetricData from a CSV row."""

        def safe_get(key: str) -> str | None:
            """Safely get string value from row, handling NaN values."""
            value = row.get(key, "")
            if pd.isna(value) or value == "":
                return None
            return str(value).strip() or None

        return cls(
            measure=safe_get("Measure") or "",
            metric_name=safe_get("Metric Name"),
            definition=safe_get("Definition"),
            cube=safe_get("Canonical Cube"),
            anchor_date=safe_get("Anchor Date"),
            filters=safe_get("Filters"),
            owning_team=safe_get("Owning Team(s)"),
        )


class MetricGeneratorService:
    """Service for generating metrics from cube definitions with CSV enhancement."""

    def __init__(self):
        self.settings = get_settings()

    def from_yaml_file(self, yaml_file_path: str, csv_file_path: str | Path | None = None) -> list[Metric]:
        """
        Generate metrics from a YAML file.

        Args:
            yaml_file_path: Path to the YAML file containing cube definitions
            csv_file_path: Optional path to a CSV file for filtering and enhancing metrics

        Returns:
            List of generated Metric objects
        """
        with open(yaml_file_path) as file:
            yaml_content = file.read()
        return self.from_yaml_content(yaml_content, csv_file_path)

    def from_yaml_content(self, yaml_content: str, csv_file_path: str | Path | None = None) -> list[Metric]:
        """
        Generate metrics from YAML content.

        Args:
            yaml_content: YAML content as a string
            csv_file_path: Optional path to a CSV file for filtering and enhancing metrics

        Returns:
            List of generated Metric objects
        """
        yaml_data = yaml.safe_load(yaml_content)
        cubes = [CubeDefinition(**cube) for cube in yaml_data.get("cubes", [])]

        # Parse CSV data if provided
        csv_metrics_map = {}
        if csv_file_path:
            csv_metrics_map = self._parse_csv_file(csv_file_path)

        metrics = []
        for cube in cubes:
            if cube.public:
                cube_metrics = self._generate_metrics_for_cube(cube, csv_metrics_map)
                metrics.extend(cube_metrics)

        return metrics

    def save_metrics_to_json(self, metrics: list[Metric], output_path: str | Path) -> None:
        """
        Save metrics to JSON file using existing JSON utilities.

        Args:
            metrics: List of Metric objects to save
            output_path: Path to the output JSON file
        """
        metrics_data = [self._prepare_metric_data(metric, include_metric_id=True) for metric in metrics]

        # Use existing JSON utilities for consistent serialization
        serialized_data = serialize_json(metrics_data)

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(serialized_data, f, indent=2)

        logger.info(f"Saved {len(metrics)} metrics to: {output_path}")

    async def save_metrics_to_db(self, metrics: list[Metric], tenant_id: int) -> int:
        """
        Save metrics to database using established patterns.

        Args:
            metrics: List of Metric objects to save
            tenant_id: ID of the tenant to save metrics for

        Returns:
            Number of metrics saved
        """
        # Set tenant context following metadata_upsert.py pattern
        logger.info(f"Setting tenant context, Tenant ID: {tenant_id}")
        set_tenant_id(tenant_id)

        # Validate tenant following existing pattern
        logger.info(f"Validating Tenant ID: {tenant_id}")
        await validate_tenant(self.settings, tenant_id)

        saved_count = 0
        async with get_async_session() as session:
            logger.info(f"Saving {len(metrics)} metrics to database for tenant {tenant_id}...")

            for i, metric in enumerate(metrics, 1):
                try:
                    # Use consolidated data preparation
                    defaults = self._prepare_metric_data(metric, include_metric_id=False)

                    # Use established upsert pattern
                    stmt = insert(MetricModel).values(metric_id=metric.metric_id, tenant_id=tenant_id, **defaults)
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
        Parse CSV file and create lookup map.

        Args:
            csv_file_path: Path to the CSV file to parse

        Returns:
            Dictionary mapping measure names to lists of CSVMetricData objects
        """
        try:
            # Read CSV with multi-row header - skip first row and use second row as header
            df = pd.read_csv(csv_file_path, header=1)
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

                        # Clean measure name - remove cube prefix if present
                        clean_measure = csv_data.measure.split(".")[-1] if "." in csv_data.measure else csv_data.measure

                        if clean_measure not in csv_metrics_map:
                            csv_metrics_map[clean_measure] = []
                        csv_metrics_map[clean_measure].append(csv_data)

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
        self, cube: CubeDefinition, csv_metrics_map: dict[str, list[CSVMetricData]] | None = None
    ) -> list[Metric]:
        """
        Generate metrics for a cube definition.

        Args:
            cube: CubeDefinition object containing cube measures and dimensions
            csv_metrics_map: Optional dictionary mapping measure names to CSVMetricData objects

        Returns:
            List of generated Metric objects
        """
        csv_metrics_map = csv_metrics_map or {}
        metrics = []

        time_dimension = self._find_primary_time_dimension(cube)

        for measure in cube.measures:
            try:
                # Skip measures not in CSV if CSV filtering is enabled
                if csv_metrics_map and measure.name not in csv_metrics_map:
                    logger.debug(f"Skipping measure {measure.name} - not found in CSV")
                    continue

                csv_data_list = csv_metrics_map.get(measure.name, [])

                if not csv_data_list:
                    # Create basic metric without CSV data
                    metric = self._create_metric_from_measure(cube.name, measure, time_dimension, None, None)
                    metrics.append(metric)
                else:
                    # Generate metrics for all CSV variants
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

    @staticmethod
    def from_cube_measures(
        cube_name: str,
        measures: list[CubeMeasure],
        dimensions: list[CubeDimension] = None,  # type: ignore
        csv_file_path: str | Path | None = None,
    ) -> list[Metric]:
        """
        Generate metrics from cube measures directly.

        Args:
            cube_name: Name of the cube
            measures: List of cube measures
            dimensions: List of cube dimensions (optional)
            csv_file_path: Optional path to CSV file for filtering and enhancing metrics

        Returns:
            List of generated Metric objects
        """
        service = MetricGeneratorService()
        cube = CubeDefinition(name=cube_name, measures=measures, dimensions=dimensions or [])

        # Parse CSV data if provided
        csv_metrics_map = {}
        if csv_file_path:
            csv_metrics_map = service._parse_csv_file(csv_file_path)

        return service._generate_metrics_for_cube(cube, csv_metrics_map)

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

        # Determine metric properties
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

        # Create metric using consolidated logic
        return Metric(
            metric_id=metric_id,
            label=label,
            abbreviation=metric_id,
            definition=definition,
            unit_of_measure=self._get_unit_of_measure(measure),
            unit=self._get_unit(measure),
            terms=[],  # Initialize as empty list
            complexity=self._get_complexity(measure),
            metric_expression=None,
            periods=[Granularity.DAY, Granularity.WEEK, Granularity.MONTH],
            grain_aggregation=self._get_aggregation_type(measure.type),
            aggregations=[self._get_aggregation_type(measure.type)],
            owned_by_team=owned_by_team,
            meta_data=MetricMetadata(semantic_meta=semantic_meta),
            dimensions=[],
            aim=self._get_aim(measure),
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
            time_dimension: SemanticMetaTimeDimension object containing time dimension details
            csv_data: Optional CSVMetricData object containing CSV data
        """
        # Use CSV anchor date if available
        if csv_data and csv_data.anchor_date:
            time_dimension = SemanticMetaTimeDimension(cube=cube_name, member=csv_data.anchor_date)

        # Parse filters from CSV or measure
        cube_filters = []
        if csv_data and csv_data.filters:
            cube_filters = self._parse_filters(csv_data.filters, cube_name)
        elif measure.filters:
            cube_filters = self._parse_measure_filters(measure.filters, cube_name)

        return SemanticMetaMetric(
            cube=cube_name, member=measure.name, time_dimension=time_dimension, cube_filters=cube_filters
        )

    def _parse_filters(self, filters_str: str, cube_name: str) -> list[CubeFilter]:
        """Parse filter string into CubeFilter objects.

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
                    CubeFilter(dimension=f"{cube_name}.{dimension_name}", operator="equals", values=[value])
                )
                continue

            # Handle IS NOT NULL filters
            not_null_match = re.match(r"\{(\w+)\}\s+IS\s+NOT\s+NULL", filter_part)
            if not_null_match:
                dimension_name = not_null_match.group(1)
                cube_filters.append(CubeFilter(dimension=f"{cube_name}.{dimension_name}", operator="set", values=[]))
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
                        dimension=f"{cube_name}.{dimension_name}", operator="equals", values=[value.lower() == "true"]
                    )
                )
                continue

            # Handle NOT NULL filters
            not_null_match = re.match(r"\{(\w+)\}\s+IS\s+NOT\s+NULL", sql_filter)
            if not_null_match:
                dimension_name = not_null_match.group(1)
                cube_filters.append(CubeFilter(dimension=f"{cube_name}.{dimension_name}", operator="set", values=[]))
                continue

        return cube_filters

    def _find_primary_time_dimension(self, cube: CubeDefinition) -> SemanticMetaTimeDimension:
        """Find primary time dimension for a cube.

        Args:
            cube: CubeDefinition object containing cube measures and dimensions

        Returns:
            SemanticMetaTimeDimension object containing time dimension details
        """
        time_candidates = [
            "created_at",
            "inquiry_at",
            "marketing_qualified_lead_at",
            "sales_qualified_lead_at",
            "opened_on",
            "closed_on",
        ]

        for dimension in cube.dimensions:
            if dimension.type == "time" and dimension.name in time_candidates:
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
