import pytest

from fulcrum_core.modules import ComponentDriftEvaluator
from fulcrum_core.modules.component_drift import CalculationType


@pytest.fixture(name="metric_values")
def metric_values_fixture():
    return [
        {"metric_id": "SalesDevSpend", "evaluation_value": 200, "comparison_value": 100},
        {"metric_id": "NewCust", "evaluation_value": 50, "comparison_value": 25},
        {"metric_id": "SalesSpend", "evaluation_value": 150, "comparison_value": 75},
        {"metric_id": "SalesMktSpend", "evaluation_value": 50, "comparison_value": 25},
    ]


@pytest.fixture(name="evaluator")
def evaluator_fixture(metric_values):
    return ComponentDriftEvaluator(metric_values)


def test_init(metric_values):
    evaluator = ComponentDriftEvaluator(metric_values)
    assert evaluator.metric_values["SalesDevSpend"]["evaluation_value"] == 200


def test_calculate_delta_additive():
    delta = ComponentDriftEvaluator.calculate_delta(200, 100, CalculationType.ADDITIVE)
    assert delta == 100


def test_calculate_delta_multiplicative_numerator():
    delta = ComponentDriftEvaluator.calculate_delta(4, 2, CalculationType.MULTIPLICATIVE)
    assert delta == pytest.approx(0.69, 0.01)


def test_calculate_delta_multiplicative_denominator():
    delta = ComponentDriftEvaluator.calculate_delta(2, 4, CalculationType.MULTIPLICATIVE, invert_values=True)
    # actual value is 0.6931471805599453
    assert round(delta, 2) == 0.69


def test_calculate_percent_change():
    percent_change = ComponentDriftEvaluator.calculate_percent_change(200, 100)
    assert percent_change == 1.0


def test_calculate_percent_change_zero_division():
    percent_change = ComponentDriftEvaluator.calculate_percent_change(200, 0)
    assert percent_change == float("inf")


def test_get_calculation_type():
    assert ComponentDriftEvaluator.get_calculation_type("+") == CalculationType.ADDITIVE
    assert ComponentDriftEvaluator.get_calculation_type("*") == CalculationType.MULTIPLICATIVE


def test_invalid_operator():
    with pytest.raises(ValueError):
        ComponentDriftEvaluator.get_calculation_type("%")


def test_calculate_relative_impact():
    relative_impact = ComponentDriftEvaluator.calculate_relative_impact(50, 100)
    assert relative_impact == 0.5


def test_calculate_marginal_contribution():
    marginal_contribution = ComponentDriftEvaluator.calculate_marginal_contribution(0.5, 20)
    assert marginal_contribution == 10


def test_calculate_value():
    operand_values = [{"evaluation_value": 10}, {"evaluation_value": 20}]
    value = ComponentDriftEvaluator.calculate_value(operand_values, "+", "evaluation_value")
    assert value == 30


def test_resolve_expression_values_metric(evaluator):
    metric_expression = {"type": "metric", "metric_id": "SalesDevSpend"}
    resolved = evaluator.resolve_expression_values(metric_expression)
    assert resolved["evaluation_value"] == 200
    assert resolved["comparison_value"] == 100


# Test resolve_expression_values for a simple expression
def test_resolve_expression_values_expression(evaluator):
    expression = {
        "type": "expression",
        "operator": "+",
        "operands": [
            {"type": "metric", "metric_id": "SalesDevSpend"},
            {"type": "metric", "metric_id": "NewCust"},
        ],
    }
    resolved = evaluator.resolve_expression_values(expression)
    assert resolved["evaluation_value"] == 250  # 200 + 50
    assert resolved["comparison_value"] == 125  # 100 + 25


def test_resolve_expression_values_metric_for_constant(evaluator):
    metric_expression = {
        "type": "expression",
        "operator": "+",
        "operands": [
            {"type": "metric", "metric_id": "SalesDevSpend"},
            {"type": "constant", "value": 100},
        ],
    }
    resolved = evaluator.resolve_expression_values(metric_expression)
    assert resolved["evaluation_value"] == 300  # 200 + 100
    assert resolved["comparison_value"] == 200  # 100 + 100


def test_resolve_expression_values_metric_coefficient(evaluator):
    metric_expression = {
        "type": "expression",
        "operator": "+",
        "operands": [{"type": "metric", "metric_id": "SalesDevSpend", "coefficient": 2}],
    }
    resolved = evaluator.resolve_expression_values(metric_expression)
    assert resolved["evaluation_value"] == 400  # 200 * 2
    assert resolved["comparison_value"] == 200  # 100 * 2


# Test calculate_drift_for_node for a simple metric
def test_calculate_drift_for_node_metric(evaluator):
    metric_node = {"type": "metric", "metric_id": "SalesDevSpend", "evaluation_value": 200, "comparison_value": 100}
    drift = evaluator.calculate_drift_for_node(metric_node)
    assert drift["absolute_drift"] == 100
    assert drift["percentage_drift"] == 1.0


def test_calculate_drift_complex_expression(evaluator):
    complex_expression = {
        "type": "expression",
        "operator": "/",
        "operands": [
            {
                "type": "expression",
                "operator": "+",
                "operands": [
                    {"type": "metric", "metric_id": "SalesDevSpend"},
                    {"type": "metric", "metric_id": "NewCust"},
                ],
            },
            {"type": "metric", "metric_id": "SalesSpend"},
        ],
    }
    expression = evaluator.resolve_expression_values(complex_expression)
    drift_results = evaluator.calculate_drift(expression)
    # Assert drift results based on your logic and calculations
    assert drift_results["drift"]["absolute_drift"] == pytest.approx(0)  # Replace 'expected_value'
