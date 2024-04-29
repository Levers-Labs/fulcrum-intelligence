from query_manager.core.schemas import Dimension, MetricDetail


def test_metric_schema_get_dimension(metric, dimension):
    # Prepare
    metric_detail = MetricDetail(**metric)
    dimension_id = dimension["id"]
    dimension_obj = Dimension(**dimension)

    # Execute
    result = metric_detail.get_dimension(dimension_id)

    # Assert
    assert result == dimension_obj

    # Execute
    result = metric_detail.get_dimension("dimension2")

    # Assert
    assert result is None

    # Prepare
    metric_detail.dimensions = None

    # Execute
    result = metric_detail.get_dimension(dimension_id)

    # Assert
    assert result is None
