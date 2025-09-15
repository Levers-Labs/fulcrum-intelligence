# Levers

Levers is a collection of analytics primitives and patterns for metric analysis. Part of the Fulcrum Intelligence
framework.

## Overview

Levers provides a collection of analytics primitives and patterns for analyzing metric performance, trends, and
dimensional breakdowns. It's designed to be simple, extensible, and focused on common analytics use cases.

## Features

- **Analytics Primitives**: Reusable functions for common calculations
- **Analytics Patterns**: Higher-level analysis patterns combining primitives
- **Pydantic Models**: Type-safe data validation and serialization
- **Pattern Configurations**: Flexible configuration for analysis patterns
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
    metric_id="revenue",
    data=df,  # DataFrame with time series data
    start_date="2023-01-01",
    end_date="2023-01-31",
    grain="day",
    threshold_ratio=0.05
)

print(result.status)  # "on_track" or "off_track"

# Using pattern configurations
from levers.models import PatternConfig, DataSource, DataSourceType, AnalysisWindowConfig, WindowStrategy

# Get default configuration for a pattern
config = levers.get_pattern_default_config("performance_status")

# Or create a custom configuration
custom_config = PatternConfig(
    pattern_name="performance_status",
    description="Custom performance status analysis",
    data_sources=[
        DataSource(
            source_type=DataSourceType.METRIC_WITH_TARGETS,
            is_required=True,
            data_key="data"
        )
    ],
    analysis_window=AnalysisWindowConfig(
        strategy=WindowStrategy.FIXED_TIME,
        days=90,
        min_days=30,
        max_days=180
    ),
    settings={"threshold_ratio": 0.05}
)

# Execute pattern with custom configuration
result = levers.execute_pattern(
    pattern_name="performance_status",
    analysis_window=analysis_window,
    config=custom_config,
    data=data_df
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

### Pattern Configurations

Pattern configurations allow you to customize how analysis patterns run:

#### Data Sources

Define what data a pattern needs to analyze:

```python
from levers.models import DataSource, DataSourceType

# Configuration for metric data with targets
data_source = DataSource(
    source_type=DataSourceType.METRIC_WITH_TARGETS,
    is_required=True,
    data_key="data"
)
```

Available data source types:

- `METRIC_TIME_SERIES`: Basic metric time series data
- `METRIC_WITH_TARGETS`: Metric data with target values
- `DIMENSIONAL_TIME_SERIES`: Dimensional breakdown of metric data
- `MULTI_METRIC`: Multiple related metrics

#### Analysis Window Strategies

Configure how time windows are determined for analysis:

```python
from levers.models import AnalysisWindowConfig, WindowStrategy

# Fixed time window (e.g., last 90 days)
window_config = AnalysisWindowConfig(
    strategy=WindowStrategy.FIXED_TIME,
    days=90,
    min_days=30,
    max_days=180
)

# Grain-specific time windows
grain_specific_config = AnalysisWindowConfig(
    strategy=WindowStrategy.GRAIN_SPECIFIC_TIME,
    grain_days={
        "day": 60,
        "week": 90,
        "month": 180,
        "quarter": 365
    },
    min_days=30,
    max_days=365
)

# Fixed number of data points
datapoints_config = AnalysisWindowConfig(
    strategy=WindowStrategy.FIXED_DATAPOINTS,
    datapoints=100,
    min_days=30,
    max_days=365
)
```

#### Complete Pattern Configuration

Combine data sources and window configuration:

```python
from levers.models import PatternConfig

config = PatternConfig(
    pattern_name="performance_status",
    version="1.0",
    description="Performance status analysis",
    data_sources=[data_source],
    analysis_window=window_config,
    settings={"threshold_ratio": 0.05}
)
```

## Extending Levers

Levers is designed to be easily extensible. You can add your own primitives and patterns to meet your specific analytics
needs.

### Adding a New Primitive

Primitives are simple functions that perform specific calculations. To add a new primitive:

1. **Choose the appropriate family**: Place your primitive in an existing module (
   e.g., `performance.py`, `trend_analysis.py`) or create a new one.

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

2. **Define the pattern configuration**: Create a default configuration for your pattern:

```python
# In patterns/your_pattern.py
from levers.models import PatternConfig, DataSource, DataSourceType, AnalysisWindowConfig, WindowStrategy


def get_default_config() -> PatternConfig:
    """Get the default configuration for the pattern."""
    return PatternConfig(
        pattern_name="your_pattern",
        description="Description of your pattern",
        data_sources=[
            DataSource(
                source_type=DataSourceType.METRIC_TIME_SERIES,
                is_required=True,
                data_key="data"
            )
        ],
        analysis_window=AnalysisWindowConfig(
            strategy=WindowStrategy.FIXED_TIME,
            days=90,
            min_days=30,
            max_days=180
        ),
        settings={"some_setting": "default_value"}
    )
```

3. **Implement the pattern class**: Create a new file in the `patterns/` directory:

```python
# In patterns/your_pattern.py
from typing import Any, Dict, Optional

from levers.exceptions import ValidationError
from levers.models.pattern_config import PatternConfig
from levers.models.patterns.your_pattern import YourPatternOutput
from levers.patterns.base import Pattern
from levers.primitives import primitive1, primitive2


class YourPattern(Pattern[YourPatternOutput]):
    """Description of your pattern."""

    name = "your_pattern"
    description = "Detailed description of what your pattern does"
    required_primitives = ["primitive1", "primitive2"]
    output_model = YourPatternOutput

    def __init__(self, config: Optional[PatternConfig] = None) -> None:
        """Initialize the pattern with optional configuration."""
        super().__init__(config=config)

    @classmethod
    def get_default_config(cls) -> PatternConfig:
        """Get the default configuration for this pattern."""
        return get_default_config()

    def analyze(self, metric_id: str, data: pd.DataFrame, analysis_window: AnalysisWindow,
                **kwargs) -> YourPatternOutput:
        """
        Execute your pattern.

        Args:
            metric_id: The ID of the metric being analyzed
            data: DataFrame containing the metric data
            analysis_window: AnalysisWindow object specifying the analysis time window
            **kwargs: Additional pattern-specific parameters

        Returns:
            YourPatternOutput with analysis results
        """
        # Validate required inputs
        self.validate_data(data, ["date", "value"])

        # Preprocess data based on analysis window
        processed_data = self.preprocess_data(data, analysis_window)

        # Handle empty data case
        if processed_data.empty:
            return self.handle_empty_data(metric_id, analysis_window)

        # Extract settings from config or kwargs
        some_setting = kwargs.get("some_setting", self.config.settings.get("some_setting") if self.config else None)

        # Perform analysis using primitives
        try:
            result_value = primitive1(processed_data["value"].values)

            # Create and validate output
            result = YourPatternOutput(
                metric_id=metric_id,
                result_value=result_value,
                metadata={}
            )

            return result

        except Exception as e:
            raise ValidationError(
                f"Error in pattern calculation: {str(e)}",
                {"pattern": self.name, "metric_id": metric_id}
            )
```

4. **Register the pattern**: Patterns are automatically discovered by the registry system.

5. **Add tests**: Create tests for your new pattern in the `tests/` directory.

### Best Practices

1. **Pattern Configuration**:
    - Define clear default configurations for patterns
    - Use standardized data source types and keys
    - Choose appropriate analysis window strategies
    - Document configuration parameters

2. **Primitive Design**:
    - Keep primitives focused on a single responsibility
    - Include proper input validation
    - Document with clear docstrings including Family and Version
    - Use appropriate error handling with custom exceptions

3. **Pattern Design**:
    - Define clear output models with Pydantic
    - Validate all inputs before processing
    - Use existing primitives when possible
    - Handle errors gracefully with context

4. **Testing**:
    - Write unit tests for primitives
    - Write integration tests for patterns
    - Test edge cases and error conditions

## Development

1. Clone the repository
2. Install dependencies: `poetry install`
3. Run tests: `poetry run pytest`
