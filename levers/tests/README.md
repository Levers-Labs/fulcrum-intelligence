# Levers Package Tests

This directory contains tests for the `levers` package. The tests are organized as follows:

## Directory Structure

```
tests/
├── conftest.py         # Common fixtures for all tests
├── unit/               # Unit tests
│   ├── conftest.py     # Fixtures specific to unit tests
│   ├── primitives/     # Tests for primitives module
│   │   ├── test_numeric.py
│   │   ├── test_performance.py
│   │   └── test_primitives_utils.py
│   └── ...             # Tests for other modules
└── ...                 # Other test types (integration, etc.)
```

## Test Types

- **Unit Tests**: Tests for individual functions and classes in isolation.
- **Integration Tests**: Tests for interactions between components.

## Running Tests

To run all tests:

```bash
cd levers
pytest
```

To run specific test files:

```bash
pytest tests/unit/primitives/test_numeric.py
```

To run tests with coverage:

```bash
pytest --cov=levers
```

## Test Fixtures

Fixtures are defined in `conftest.py` files at different levels:

- `tests/conftest.py`: Common fixtures for all tests
- `tests/unit/conftest.py`: Fixtures specific to unit tests

## Test Naming Conventions

- Test files: `test_*.py`
- Test classes: `Test*`
- Test functions: `test_*`

## Test Structure

Each test follows the Arrange-Act-Assert (AAA) pattern:

```python
def test_something():
    # Arrange
    input_value = ...
    expected_output = ...

    # Act
    result = function_under_test(input_value)

    # Assert
    assert result == expected_output
```

## Adding New Tests

When adding new tests:

1. Follow the existing directory structure
2. Use appropriate fixtures from conftest.py
3. Follow the AAA pattern
4. Add docstrings to test functions
5. Ensure comprehensive test coverage
