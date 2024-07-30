from query_manager.core.models import Dimension, Metric


def test_metric_schema_get_dimension(metric, dimension):
    # Prepare
    metric = Metric.model_validate(metric)
    dimension_id = dimension["dimension_id"]
    dimension_obj = Dimension(**dimension)
    metric.dimensions = [dimension_obj]

    # Execute
    result = metric.get_dimension(dimension_id)

    # Assert
    assert result == dimension_obj

    # Execute
    result = metric.get_dimension("dimension2")

    # Assert
    assert result is None

    # Prepare
    metric.dimensions = []

    # Execute
    result = metric.get_dimension(dimension_id)

    # Assert
    assert result is None
