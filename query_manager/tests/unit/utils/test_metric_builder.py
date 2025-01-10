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
        aggregation: sum
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
