import yaml
from fastapi import HTTPException
from pydantic import BaseModel, field_validator

from query_manager.core.enums import MetricAim
from query_manager.core.models import CubeFilter, MetricExpression
from query_manager.core.schemas import MetricCreate


class MetricBase(BaseModel):
    """Base metric fields that are required"""

    metric_id: str
    label: str
    abbreviation: str
    hypothetical_max: float
    measure: str
    time_dimension: str

    @field_validator("measure", "time_dimension")
    @classmethod
    def validate_cube_format(cls, v: str) -> str:
        """Validate cube.member format"""
        cube, *member = v.split(".")
        if not cube or not member:
            raise ValueError(f"Must be in format 'Cube.member', got {v}")
        return v


class MetricDataBuilder:
    @staticmethod
    async def build_metric_data(metric_data: str, client, expression_parser_service) -> MetricCreate:
        """
        Builds metric data structure from input content. This method parses the input YAML content,
        validates the required fields, fetches measure details and dimensions, builds the metric expression
        if provided, and finally constructs the metric data structure.
        """
        # Parse and validate content
        data = MetricDataBuilder._parse_content(metric_data)
        MetricDataBuilder._validate_required_fields(data)

        # Get measure details and dimensions
        measure_details, dimensions = await MetricDataBuilder._get_measure_and_dimensions(
            client, data["measure"], data["time_dimension"]
        )

        # Build expression if exists
        components: list[str] = []
        metric_expression = None
        if data.get("expression"):
            components, metric_expression = await MetricDataBuilder._build_expression(
                expression_parser_service, data.get("expression"), data["metric_id"]  # type: ignore
            )

        cube_filters = None
        if "cube_filters" in data:
            cube_name = data["measure"].split(".")[0]
            cube_filters = MetricDataBuilder._build_cube_filter(data["cube_filters"], cube_name)

        # Build final metric structure
        return MetricDataBuilder._build_metric_structure(
            data=data,
            measure=data["measure"],
            measure_details=measure_details,
            time_dimension=data["time_dimension"],
            dimensions=dimensions,
            components=components,
            metric_expression=metric_expression,
            cube_filters=cube_filters,
        )

    @staticmethod
    def _parse_content(content: str) -> dict:
        """
        Parses input content into a dictionary. This method attempts to load the YAML content into a dictionary.
        If the content is invalid or missing, it raises an HTTPException.
        """
        try:
            data = yaml.safe_load(content)
            if not data:
                raise HTTPException(status_code=422, detail="Invalid content provided")
            return data
        except yaml.YAMLError as e:
            raise HTTPException(status_code=422, detail=f"Invalid format: {str(e)}") from e

    @staticmethod
    def _validate_required_fields(data: dict) -> None:
        """
        Validates required fields exist in the data. This method checks if all required fields are present in the data.
        If any required field is missing, it raises an HTTPException.
        """
        try:
            _ = MetricBase(**data)
        except ValueError as e:
            raise HTTPException(status_code=422, detail=str(e)) from e

    @staticmethod
    async def _get_measure_and_dimensions(client, measure: str, time_dimension: str) -> tuple[dict, list[str]]:
        """
        Gets measure details and available dimensions with validation. This method fetches the measure details and
        available dimensions from the client. It validates the format of the measure and time dimension, ensuring they
        are in the correct format. If the format is incorrect or the measure or time dimension is not found, it raises
        an HTTPException.
        """
        measure_cube, measure_name = measure.split(".")
        time_cube, time_member = time_dimension.split(".")

        if not all([measure_cube, measure_name, time_cube, time_member]):
            raise HTTPException(status_code=422, detail="Measure and time_dimension must be in format 'Cube.member'")

        cubes = await client.list_cubes(cube_name=measure_cube)
        if not cubes:
            raise HTTPException(status_code=404, detail=f"Cube {measure_cube} not found")

        cube = cubes[0]
        measure_details = next((m for m in cube["measures"] if m["name"] == measure), None)
        if not measure_details:
            raise HTTPException(status_code=422, detail=f"Measure '{measure}' not found in cube {measure_cube}")

        time_dimension_exists = any(
            dimension["name"] == time_dimension
            for dimension in cube.get("dimensions", [])
            if dimension["type"] == "time"
        )
        if not time_dimension_exists:
            raise HTTPException(
                status_code=422, detail=f"Time dimension '{time_dimension}' not found in cube {measure_cube}"
            )

        # TODO: should we filter out dimensions here? based on the length of values? for now, not including dimensions
        dimensions: list = []  # type: ignore

        return measure_details, dimensions

    @staticmethod
    async def _build_expression(parser_service, expression: str, metric_id: str) -> tuple[list[str], dict | None]:
        """
        Builds metric expression and extracts components. This method processes the expression using the parser service,
        extracts the components (metric IDs) from the parsed expression, and constructs a MetricExpression object.
        If the expression is invalid, it raises an HTTPException.
        """
        try:
            parsed_expression = await parser_service.process(expression)
            components = list(
                {
                    metric["metric_id"]
                    for metric in parsed_expression.expression.dict().get("operands", [])
                    if metric.get("type") == "metric"
                }
            )

            metric_expression = MetricExpression(
                metric_id=metric_id, expression_str=expression, expression=parsed_expression.expression.dict()
            )

            return components, metric_expression.dict()
        except Exception as e:
            raise HTTPException(status_code=422, detail=f"Invalid expression: {str(e)}") from e

    @staticmethod
    def _build_metric_structure(
        data: dict,
        measure: str,
        measure_details: dict,
        time_dimension: str,
        dimensions: list[str],
        components: list[str] | None = None,
        metric_expression: dict | None = None,
        cube_filters: list[CubeFilter] | None = None,
    ) -> MetricCreate:
        """Build the final metric data structure"""
        # Extract locations once
        measure_cube, measure_name = measure.split(".")
        time_cube, time_member = time_dimension.split(".")

        # Get aggregation once
        aggregation = data.get("aggregation", measure_details["grain_aggregation"])

        # Prepare semantic metadata
        semantic_meta = {
            "cube": measure_cube,
            "member": measure_name,
            "member_type": "measure",
            "time_dimension": {"cube": time_cube, "member": time_member},
            "cube_filters": cube_filters,
        }

        # Build metric structure
        return MetricCreate(
            # Required fields
            metric_id=data["metric_id"],
            label=data["label"],
            abbreviation=data["abbreviation"],
            hypothetical_max=data["hypothetical_max"],
            # Optional fields with defaults
            definition=data.get("definition", ""),
            unit_of_measure=data.get("unit_of_measure", ""),
            unit=data.get("unit", ""),
            terms=[],
            complexity="Complex" if metric_expression else "Atomic",
            metric_expression=metric_expression,
            periods=["day"],
            grain_aggregation=aggregation,
            aggregations=[aggregation],
            owned_by_team=[],
            meta_data={"semantic_meta": semantic_meta},
            dimensions=dimensions,
            influences=data.get("influences", []),
            components=components,
            inputs=data.get("inputs", []),
            aim=data.get("aim").title() if data.get("aim") else MetricAim.MAXIMIZE,  # type: ignore
        )

    @staticmethod
    def _build_cube_filter(cube_filters: list, cube_name: str) -> list[CubeFilter]:
        """Build cube filter from a list of filter tuples"""
        if not cube_filters:
            return []

        result = []
        for i, filter_item in enumerate(cube_filters):
            # Validate filter item structure
            if not isinstance(filter_item, list) or len(filter_item) != 3:
                raise HTTPException(
                    status_code=422,
                    detail=f"Invalid cube filter format at index {i}. Expected [dimension, operator, values] but got "
                    f"{filter_item}",
                )

            dimension, operator, values = filter_item

            # Validate dimension
            if not isinstance(dimension, str):
                raise HTTPException(
                    status_code=422,
                    detail=f"Invalid dimension at index {i}. Expected string but got {type(dimension).__name__}",
                )

            # Validate values is a list
            if not isinstance(values, list):
                raise HTTPException(
                    status_code=422,
                    detail=f"Invalid values at index {i}. Expected list but got {type(values).__name__}",
                )

            result.append(CubeFilter(dimension=f"{cube_name}.{dimension}", operator=operator, values=values))

        return result
