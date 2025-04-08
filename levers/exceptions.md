# docs/exceptions.md
# Levers Library Exception Handling

The Levers Library uses a structured exception hierarchy to provide clear error information and diagnostics.

## Exception Hierarchy

- **LeversError** - Base exception for all library errors
  - **ValidationError** - Input validation failures
  - **DataError** - Data-related issues
    - **MissingDataError** - Required data is missing
    - **InvalidDataError** - Data is invalid or incompatible
    - **TimeSeriesError** - Time series-specific issues
      - **TimeRangeError** - Time range is invalid or contains no data
  - **CalculationError** - Calculation failures
    - **DivisionByZeroError** - Division by zero errors
  - **PatternError** - Pattern-specific errors
  - **PrimitiveError** - Primitive-specific errors

## Handling Exceptions

### Basic Exception Handling

```python
from levers import Levers
from levers.exceptions import LeversError

try:
    result = Levers.analyze_performance_status(
        metric_id="daily_active_users",
        current_value=105,
        target_value=0  # This will cause an issue with division by zero
    )
except LeversError as e:
    print(f"Error: {e.message}")
    print(f"Details: {e.details}")
```

### Specific Exception Types

```python
from levers import calculate_metric_gva
from levers.exceptions import ValidationError, CalculationError

try:
    result = calculate_metric_gva(
        actual_value="not_a_number",  # Invalid input
        target_value=100
    )
except ValidationError as e:
    print(f"Validation error: {e.message}")
    print(f"Invalid fields: {e.invalid_fields}")
except CalculationError as e:
    print(f"Calculation error: {e.message}")
except Exception as e:
    print(f"Other error: {str(e)}")
```

## Common Exception Messages and Solutions

### ValidationError

- **Message**: "Invalid input for [primitive/pattern]"
- **Solution**: Check input types and values against documentation requirements

### MissingDataError

- **Message**: "Required columns missing from DataFrame"
- **Solution**: Ensure your DataFrame includes all required columns specified in the documentation

### TimeRangeError

- **Message**: "No data in specified date range"
- **Solution**: Verify your date range contains data or adjust the range

### DivisionByZeroError

- **Message**: "Cannot divide by zero"
- **Solution**: Add special handling for zero values in your inputs
