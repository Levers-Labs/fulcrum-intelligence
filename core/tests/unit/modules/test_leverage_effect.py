import pytest
from sympy import Symbol
from core.fulcrum_core.modules.leverage_effect import LeverageCalculator  # Replace 'your_module' with the actual module name where LeverageCalculator is defined

@pytest.fixture(name="calculator")
def fixture_calculator():
    # Prepare the data for LeverageCalculator
    equation_str = "a * b + c * d - e"
    variables = ["a", "b", "c", "d", "e"]
    values = {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}
    max_values = {"a": 3, "b": 4, "c": 5, "d": 6, "e": 7}
    
    # Initialize LeverageCalculator
    calculator = LeverageCalculator(equation_str, variables, values, max_values)
    return calculator

def test_compute_y(calculator):
    # Act
    result = calculator._compute_y()
    
    # Assert
    expected_value = 1 * 2 + 3 * 4 - 5
    assert result == expected_value

def test_compute_ymax(calculator):
    # Act
    result_a = calculator._compute_ymax("a", 3)
    result_b = calculator._compute_ymax("b", 4)
    result_c = calculator._compute_ymax("c", 5)
    result_d = calculator._compute_ymax("d", 6)
    result_e = calculator._compute_ymax("e", 7)
    
    # Assert
    expected_a = 3 * 2 + 3 * 4 - 5
    expected_b = 1 * 4 + 3 * 4 - 5
    expected_c = 1 * 2 + 5 * 6 - 5
    expected_d = 1 * 2 + 3 * 6 - 5
    expected_e = 1 * 2 + 3 * 4 - 7
    
    assert result_a == expected_a
    assert result_b == expected_b
    assert result_c == expected_c
    assert result_d == expected_d
    assert result_e == expected_e

def test_analyze(calculator):
    # Act
    results = calculator.analyze()
    
    # Assert
    assert len(results) == 5
    assert results[0]["variable"] == "b"
    assert results[0]["percentage_difference"] == pytest.approx(50.0, rel=1e-2)
    assert results[1]["variable"] == "d"
    assert results[1]["percentage_difference"] == pytest.approx(33.33, rel=1e-2)
    assert results[2]["variable"] == "c"
    assert results[2]["percentage_difference"] == pytest.approx(25.0, rel=1e-2)
    assert results[3]["variable"] == "a"
    assert results[3]["percentage_difference"] == pytest.approx(50.0, rel=1e-2)
    assert results[4]["variable"] == "e"
    assert results[4]["percentage_difference"] == pytest.approx(40.0, rel=1e-2)
