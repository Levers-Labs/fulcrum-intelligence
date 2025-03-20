# Levers

Levers is a collection of analytics primitives and patterns for metric analysis. Part of the Fulcrum Intelligence framework.

## Overview

Levers provides a collection of analytics primitives and patterns for analyzing metric performance, trends, and dimensional breakdowns. It's designed to be simple, extensible, and focused on common analytics use cases.

## Features

- **Analytics Primitives**: Reusable functions for common calculations
- **Analytics Patterns**: Higher-level analysis patterns combining primitives
- **Pydantic Models**: Type-safe data validation and serialization
- **Extensible Design**: Easy to add new primitives and patterns
- **Clear Error Handling**: Custom exceptions for different error cases

## Installation

```bash
pip install levers
```

Or with Poetry:

```bash
poetry add levers
```

## Quick Start

```python
from levers import Levers
from datetime import datetime

# Initialize the API
levers = Levers()

# Use the convenient analysis methods
result = levers.analyze_performance_status(
    metric_name="revenue",
    current_value=105.0,
    target_value=100.0,
    evaluation_date=datetime.now()
)

print(result["status"])  # "above_target"
print(result["difference_percentage"])  # 5.0

# For time series analysis
import pandas as pd
df = pd.DataFrame({
    "date": pd.date_range(start="2023-01-01", periods=10),
    "value": [100, 102, 105, 103, 107, 110, 112, 115, 118, 120]
})

historical_result = levers.analyze_historical_performance(
    metric_name="revenue",
    time_series=df,
    start_date=datetime(2023, 1, 1),
    end_date=datetime(2023, 1, 10)
)
```

## Documentation

### Patterns

- **Performance Status**: Analyze metric performance against targets
- **Historical Performance**: Track performance changes over time
- **Dimensional Analysis**: Break down metrics by dimensions

### Primitives

- **Performance**: Basic metric calculations
- **Time Series Growth**: Growth rate calculations
- **Trend Analysis**: Trend detection and analysis
- **Descriptive Stats**: Statistical calculations

## Extending Levers

Levers is designed to be easily extensible. You can add your own primitives and patterns to meet your specific analytics needs.

### Adding a New Primitive

Primitives are simple functions that perform specific calculations. To add a new primitive:

1. **Choose the appropriate family**: Place your primitive in an existing module (e.g., `performance.py`, `trend_analysis.py`) or create a new one.

2. **Create your function**: Follow this template:

```python
def my_new_primitive(param1: float, param2: float) -> float:
    """
    Brief description of what the primitive does.

    Family: FamilyName
    Version: 1.0

    Args:
        param1: Description of param1
        param2: Description of param2

    Returns:
        Description of return value
    """
    # Input validation
    if param1 < 0:
        raise ValidationError("param1 must be non-negative", {"field": "param1"})

    # Calculation logic
    result = param1 + param2

    return result
```

3. **Add to `__init__.py`**: Make sure your primitive is imported and exposed in the module's `__init__.py` file:

```python
from levers.primitives.your_module import my_new_primitive

__all__ = [
    # ... existing primitives
    "my_new_primitive",
]
```

### Adding a New Pattern

Patterns combine primitives to perform higher-level analysis. To add a new pattern:

1. **Create the output model**: Define a Pydantic model in the appropriate file under `models/patterns/`:

```python
# In models/patterns/your_pattern.py
from pydantic import BaseModel, Field
from typing import Any, Dict

class YourPatternOutput(BaseModel):
    """Output model for your pattern."""
    metric_name: str
    result_value: float
    metadata: Dict[str, Any] = Field(default_factory=dict)
```

2. **Implement the pattern class**: Create a new file in the `patterns/` directory:

```python
# In patterns/your_pattern.py
from typing import Any, Dict

from levers.exceptions import ValidationError
from levers.models.patterns.your_pattern import YourPatternOutput
from levers.patterns.base import BasePattern
from levers.primitives import primitive1, primitive2

class YourPattern(BasePattern):
    """Description of your pattern."""

    name = "YourPattern"
    description = "Detailed description of what your pattern does"
    required_primitives = ["primitive1", "primitive2"]
    output_model = YourPatternOutput

    def analyze(self, input_data: Dict[str, Any]) -> YourPatternOutput:
        """
        Execute your pattern.

        Args:
            input_data: Dictionary with input parameters

        Returns:
            YourPatternOutput with analysis results
        """
        # Validate required inputs
        required_keys = {"metric_name", "input_value"}
        self.check_required_data(input_data, required_keys)

        # Extract inputs
        metric_name = input_data["metric_name"]
        input_value = float(input_data["input_value"])
        metadata = input_data.get("metadata", {})

        # Perform analysis using primitives
        try:
            result_value = primitive1(input_value)

            # Create and validate output
            result = {
                "metric_name": metric_name,
                "result_value": result_value,
                "metadata": metadata,
            }

            return self.validate_output(result)

        except Exception as e:
            raise ValidationError(
                f"Error in pattern calculation: {str(e)}",
                {"pattern": self.name, "inputs": {"input_value": input_value}}
            )
```

3. **Register the pattern**: Patterns are automatically discovered by the registry system.

4. **Add tests**: Create tests for your new pattern in the `tests/` directory.

### Best Practices

1. **Primitive Design**:
   - Keep primitives focused on a single responsibility
   - Include proper input validation
   - Document with clear docstrings including Family and Version
   - Use appropriate error handling with custom exceptions

2. **Pattern Design**:
   - Define clear output models with Pydantic
   - Validate all inputs before processing
   - Use existing primitives when possible
   - Handle errors gracefully with context

3. **Testing**:
   - Write unit tests for primitives
   - Write integration tests for patterns
   - Test edge cases and error conditions

## Development

1. Clone the repository
2. Install dependencies: `poetry install`
3. Run tests: `poetry run pytest`
