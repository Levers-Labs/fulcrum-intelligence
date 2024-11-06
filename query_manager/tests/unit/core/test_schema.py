from copy import deepcopy

import pytest
from sqlalchemy import select

from commons.models.enums import Granularity
from commons.utilities.context import set_tenant_id
from query_manager.core.enums import Complexity
from query_manager.core.models import Dimension, Metric
from query_manager.core.schemas import (
    DimensionCreate,
    DimensionUpdate,
    MetricCreate,
    MetricUpdate,
)


@pytest.mark.asyncio
async def test_dimension_create(db_session, jwt_payload):
    # Prepare test data
    dimension_data = {
        "tenant_id": 1,
        "dimension_id": "test_dimension",
        "label": "Test Dimension",
        "definition": "A test dimension",
        "meta_data": {
            "semantic_meta": {
                "member_type": "dimension",
                "cube": "cube",
                "member": "dimension",
            },
        },
    }
    # set tenant_id
    set_tenant_id(jwt_payload["tenant_id"])

    # Call the create method
    result = await DimensionCreate.create(db_session, dimension_data)

    # Assertions
    assert isinstance(result, Dimension)
    assert result.dimension_id == "test_dimension"
    assert result.label == "Test Dimension"
    assert result.definition == "A test dimension"

    # Verify the result in the database
    db_dimension = await db_session.execute(select(Dimension).filter_by(dimension_id="test_dimension"))
    db_dimension = db_dimension.scalar_one()
    assert db_dimension is not None
    assert db_dimension.label == "Test Dimension"


@pytest.mark.asyncio
async def test_dimension_update(db_session, jwt_payload):
    # Prepare test data
    dimension_data = {
        "tenant_id": 1,
        "dimension_id": "test_dimension_update",
        "label": "Test Dimension",
        "reference": "test_dimension",
        "definition": "A test dimension",
        "meta_data": {
            "semantic_meta": {
                "member_type": "dimension",
                "cube": "cube",
                "member": "dimension",
            },
        },
    }
    # set tenant_id
    set_tenant_id(jwt_payload["tenant_id"])

    update_data = deepcopy(dimension_data)

    instance = Dimension(**dimension_data)
    db_session.add(instance)
    await db_session.flush()

    update_data["label"] = "Updated Label"

    # Call the update method
    result = await DimensionUpdate.update(db_session, instance, update_data)

    # Assertions
    assert isinstance(result, Dimension)
    assert result.dimension_id == "test_dimension_update"
    assert result.label == "Updated Label"

    # Verify the result in the database
    db_dimension = await db_session.execute(select(Dimension).filter_by(dimension_id="test_dimension_update"))
    db_dimension = db_dimension.scalar_one()
    assert db_dimension is not None
    assert db_dimension.label == "Updated Label"


@pytest.mark.asyncio
async def test_metric_create(db_session, jwt_payload):
    # Prepare test data
    metric_data = {
        "tenant_id": 1,
        "metric_id": "test_metric",
        "label": "Test Metric",
        "abbreviation": "TM",
        "definition": "A test metric",
        "unit_of_measure": "Count",
        "unit": "#",
        "complexity": Complexity.ATOMIC,
        "metric_expression": {
            "expression_str": "{TestMetric}",
            "metric_id": "TestMetric",
            "type": "metric",
            "period": 0,
        },
        "grain_aggregation": "sum",
        "terms": ["term1", "term2"],
        "periods": [Granularity.DAY, Granularity.WEEK],
        "aggregations": ["sum", "avg"],
        "owned_by_team": ["team1"],
        "meta_data": {
            "semantic_meta": {
                "cube": "test_cube",
                "member": "test_metric",
                "member_type": "metric",
            },
        },
    }

    # set tenant_id
    set_tenant_id(jwt_payload["tenant_id"])

    # Call the create method
    result = await MetricCreate.create(db_session, metric_data)

    # Assertions
    assert isinstance(result, Metric)
    assert result.metric_id == "test_metric"
    assert result.label == "Test Metric"
    assert result.abbreviation == "TM"
    assert result.definition == "A test metric"
    assert result.unit_of_measure == "Count"
    assert result.unit == "#"
    assert result.complexity == Complexity.ATOMIC
    assert result.grain_aggregation == "sum"
    assert result.terms == ["term1", "term2"]
    assert result.periods == [Granularity.DAY, Granularity.WEEK]
    assert result.aggregations == ["sum", "avg"]
    assert result.owned_by_team == ["team1"]
    assert result.meta_data == {
        "semantic_meta": {
            "cube": "test_cube",
            "member": "test_metric",
            "member_type": "metric",
        },
    }
    # Verify the result in the database
    db_metric = await db_session.execute(select(Metric).filter_by(metric_id="test_metric"))
    db_metric = db_metric.scalar_one()
    assert db_metric is not None
    assert db_metric.label == "Test Metric"


@pytest.mark.asyncio
async def test_metric_update(db_session, jwt_payload):
    # Prepare test data
    metric_data = {
        "tenant_id": 1,
        "metric_id": "test_metric_update",
        "label": "Test Metric",
        "abbreviation": "TM",
        "definition": "A test metric",
        "unit_of_measure": "Count",
        "unit": "#",
        "complexity": Complexity.ATOMIC,
        "metric_expression": {
            "expression_str": "{TestMetric}",
            "metric_id": "TestMetric",
            "type": "metric",
            "period": 0,
        },
        "grain_aggregation": "sum",
        "terms": ["term1", "term2"],
        "periods": [Granularity.DAY, Granularity.WEEK],
        "aggregations": ["sum", "avg"],
        "owned_by_team": ["team1"],
        "meta_data": {
            "semantic_meta": {
                "cube": "test_cube",
                "member": "test_metric",
                "member_type": "metric",
            },
        },
    }
    # set tenant_id
    set_tenant_id(jwt_payload["tenant_id"])

    instance = Metric(**metric_data)
    db_session.add(instance)
    await db_session.commit()

    update_data = {
        "label": "Updated Metric",
        "abbreviation": "UM",
        "definition": "An updated test metric",
        "unit_of_measure": "Percentage",
        "unit": "%",
        "complexity": Complexity.COMPLEX,
        "grain_aggregation": "avg",
        "terms": ["term3"],
        "periods": [Granularity.MONTH],
        "aggregations": ["max"],
        "owned_by_team": ["team2"],
        "meta_data": {
            "semantic_meta": {
                "cube": "updated_cube",
                "member": "updated_metric",
                "member_type": "metric",
            },
        },
    }

    # Call the update method
    result = await MetricUpdate.update(db_session, instance, update_data)

    # Assertions
    assert isinstance(result, Metric)
    assert result.metric_id == "test_metric_update"
    assert result.label == "Updated Metric"
    assert result.abbreviation == "UM"
    assert result.definition == "An updated test metric"
    assert result.unit_of_measure == "Percentage"
    assert result.unit == "%"
    assert result.complexity == Complexity.COMPLEX
    assert result.grain_aggregation == "avg"
    assert result.terms == ["term3"]
    assert result.periods == [Granularity.MONTH]
    assert result.aggregations == ["max"]
    assert result.owned_by_team == ["team2"]
    assert result.meta_data == {
        "semantic_meta": {
            "cube": "updated_cube",
            "member": "updated_metric",
            "member_type": "metric",
        },
    }

    # Verify the result in the database
    db_metric = await db_session.execute(select(Metric).filter_by(metric_id="test_metric_update"))
    db_metric = db_metric.scalar_one()
    assert db_metric is not None
    assert db_metric.label == "Updated Metric"
