import pytest
from fastapi import HTTPException

from query_manager.utils.metric_builder import MetricDataBuilder


@pytest.fixture
def valid_yaml_content():
    return """
        metric_id: test
        label: test
        abbreviation: test
        hypothetical_max: 100
        measure: cube1.revenue
        time_dimension: cube1.created_at
        definition: test is a metric
        unit_of_measure: quantity
        unit: n
        expression: "{newInqs} + {newMqls} + 1"
        aggregation: sum,
        aim: Maximize
    """


@pytest.fixture
def mock_cube_data():
    return [
        {
            "name": "cube1",
            "measures": [{"name": "cube1.revenue", "grain_aggregation": "sum"}],
            "dimensions": [{"name": "cube1.created_at", "type": "time", "dimension_id": "time"}],
        }
    ]


@pytest.mark.asyncio
async def test_build_metric_structure(mock_cube_data):
    """Test _build_metric_structure method"""
    data = {
        "metric_id": "test",
        "label": "test",
        "abbreviation": "test",
        "hypothetical_max": 100,
        "definition": "test definition",
        "unit_of_measure": "quantity",
        "unit": "n",
        "aggregation": "sum",
        "aim": "Maximize",
    }
    measure = "cube1.revenue"
    measure_details = {"grain_aggregation": "sum"}
    time_dimension = "cube1.created_at"
    dimensions = ["dimension1"]

    result = MetricDataBuilder._build_metric_structure(
        data=data,
        measure=measure,
        measure_details=measure_details,
        time_dimension=time_dimension,
        dimensions=dimensions,
    )

    assert result.metric_id == "test"
    assert result.label == "test"
    assert result.complexity == "Atomic"
    assert result.grain_aggregation == "sum"
    assert result.dimensions == ["dimension1"]
    assert result.definition == "test definition"
    assert result.unit_of_measure == "quantity"
    assert result.unit == "n"


@pytest.mark.asyncio
async def test_build_metric_structure_with_expression():
    """Test _build_metric_structure method with expression"""
    data = {
        "metric_id": "test",
        "label": "test",
        "abbreviation": "test",
        "hypothetical_max": 100,
        "definition": "test definition",
        "unit_of_measure": "quantity",
        "unit": "n",
        "aggregation": "sum",
        "aim": "Maximize",
    }
    measure = "cube1.revenue"
    measure_details = {"grain_aggregation": "sum"}
    time_dimension = "cube1.created_at"
    dimensions = ["dimension1"]
    components = ["metric1", "metric2"]
    metric_expression = {"type": "metric", "metric_id": "metric1"}

    result = MetricDataBuilder._build_metric_structure(
        data=data,
        measure=measure,
        measure_details=measure_details,
        time_dimension=time_dimension,
        dimensions=dimensions,
        components=components,
        metric_expression=metric_expression,
    )

    assert result.complexity == "Complex"
    assert result.components == components


async def test_validate_required_fields():
    """Test _validate_required_fields method"""
    valid_data = {
        "metric_id": "test",
        "label": "test",
        "abbreviation": "test",
        "hypothetical_max": 100,
        "measure": "cube1.revenue",
        "time_dimension": "cube1.created_at",
    }

    # Should not raise exception
    MetricDataBuilder._validate_required_fields(valid_data)


async def test_validate_required_fields_missing():
    """Test _validate_required_fields method with missing fields"""
    invalid_data = {"metric_id": "test", "label": "test"}

    with pytest.raises(HTTPException) as exc:
        MetricDataBuilder._validate_required_fields(invalid_data)
    assert exc.value.status_code == 422
    assert "Missing mandatory fields" in str(exc.value.detail)


def test_parse_content_valid_yaml():
    """Test _parse_content with valid YAML."""
    yaml_content = """
    metric_id: test_metric
    label: Test Metric
    abbreviation: TM
    """

    result = MetricDataBuilder._parse_content(yaml_content)

    assert result["metric_id"] == "test_metric"
    assert result["label"] == "Test Metric"
    assert result["abbreviation"] == "TM"


def test_parse_content_invalid_yaml():
    """Test _parse_content with invalid YAML."""
    invalid_yaml = """
    metric_id: test_metric
    label: Test Metric
    abbreviation: TM
    invalid_indent
    """

    with pytest.raises(HTTPException) as exc:
        MetricDataBuilder._parse_content(invalid_yaml)
    assert exc.value.status_code == 422
    assert "Invalid format" in str(exc.value.detail)


def test_parse_content_empty_content():
    """Test _parse_content with empty content."""
    empty_yaml = ""

    with pytest.raises(HTTPException) as exc:
        MetricDataBuilder._parse_content(empty_yaml)
    assert exc.value.status_code == 422
    assert "Invalid content provided" in str(exc.value.detail)


@pytest.mark.asyncio
async def test_get_measure_and_dimensions_cube_not_found():
    """Test _get_measure_and_dimensions when cube is not found."""
    from unittest.mock import AsyncMock

    mock_client = AsyncMock()
    mock_client.list_cubes.return_value = []  # Empty list - cube not found

    with pytest.raises(HTTPException) as exc:
        await MetricDataBuilder._get_measure_and_dimensions(
            mock_client, "nonexistent_cube.revenue", "nonexistent_cube.created_at"
        )
    assert exc.value.status_code == 404
    assert "Cube nonexistent_cube not found" in str(exc.value.detail)


@pytest.mark.asyncio
async def test_get_measure_and_dimensions_measure_not_found():
    """Test _get_measure_and_dimensions when measure is not found."""
    from unittest.mock import AsyncMock

    mock_client = AsyncMock()
    mock_client.list_cubes.return_value = [
        {
            "name": "test_cube",
            "measures": [{"name": "test_cube.other_measure", "grain_aggregation": "sum"}],
            "dimensions": [{"name": "test_cube.created_at", "type": "time"}],
        }
    ]

    with pytest.raises(HTTPException) as exc:
        await MetricDataBuilder._get_measure_and_dimensions(mock_client, "test_cube.revenue", "test_cube.created_at")
    assert exc.value.status_code == 422
    assert "Measure 'test_cube.revenue' not found" in str(exc.value.detail)


@pytest.mark.asyncio
async def test_get_measure_and_dimensions_time_dimension_not_found():
    """Test _get_measure_and_dimensions when time dimension is not found."""
    from unittest.mock import AsyncMock

    mock_client = AsyncMock()
    mock_client.list_cubes.return_value = [
        {
            "name": "test_cube",
            "measures": [{"name": "test_cube.revenue", "grain_aggregation": "sum"}],
            "dimensions": [{"name": "test_cube.other_time", "type": "time"}],
        }
    ]

    with pytest.raises(HTTPException) as exc:
        await MetricDataBuilder._get_measure_and_dimensions(mock_client, "test_cube.revenue", "test_cube.created_at")
    assert exc.value.status_code == 422
    assert "Time dimension 'test_cube.created_at' not found" in str(exc.value.detail)


def test_validate_cube_format_valid():
    """Test validate_cube_format with valid format."""
    from query_manager.utils.metric_builder import MetricBase

    # Valid formats should not raise
    assert MetricBase.validate_cube_format("cube.member") == "cube.member"
    assert MetricBase.validate_cube_format("my_cube.my_member") == "my_cube.my_member"


def test_validate_cube_format_invalid():
    """Test validate_cube_format with invalid format."""
    from query_manager.utils.metric_builder import MetricBase

    with pytest.raises(ValueError) as exc:
        MetricBase.validate_cube_format("invalid_format")
    assert "Must be in format 'Cube.member'" in str(exc.value)

    with pytest.raises(ValueError) as exc:
        MetricBase.validate_cube_format(".")
    assert "Must be in format 'Cube.member'" in str(exc.value)


def test_build_cube_filter_empty_filters():
    """Test _build_cube_filter with empty filters."""
    result = MetricDataBuilder._build_cube_filter([], "test_cube")
    assert result == []


def test_build_cube_filter_invalid_structure():
    """Test _build_cube_filter with invalid filter structure."""
    invalid_filters = [["dimension1", "equals"], "invalid_structure"]  # Missing values - only 2 elements  # Not a list

    with pytest.raises(HTTPException) as exc:
        MetricDataBuilder._build_cube_filter(invalid_filters, "test_cube")
    assert exc.value.status_code == 422
    assert "Invalid cube filter format at index 0" in str(exc.value.detail)


def test_build_cube_filter_invalid_dimension():
    """Test _build_cube_filter with invalid dimension type."""
    invalid_filters = [[123, "equals", ["value1"]]]  # Dimension should be string

    with pytest.raises(HTTPException) as exc:
        MetricDataBuilder._build_cube_filter(invalid_filters, "test_cube")
    assert exc.value.status_code == 422
    assert "Invalid dimension at index 0" in str(exc.value.detail)


def test_build_cube_filter_invalid_values():
    """Test _build_cube_filter with invalid values type."""
    invalid_filters = [["dimension1", "equals", "should_be_list"]]  # Values should be list

    with pytest.raises(HTTPException) as exc:
        MetricDataBuilder._build_cube_filter(invalid_filters, "test_cube")
    assert exc.value.status_code == 422
    assert "Invalid values at index 0" in str(exc.value.detail)
