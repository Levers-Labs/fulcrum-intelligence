# primitives/__init__.py
# Import and expose all primitives for easy access

# Numeric primitives
from levers.exceptions import PrimitiveError

from .numeric import (
    calculate_difference,
    calculate_gap_to_target,
    calculate_percentage_difference,
    round_to_precision,
    safe_divide,
)

# Performance primitives
from .performance import (
    calculate_historical_gva,
    calculate_metric_gva,
    calculate_moving_target,
    calculate_required_growth,
    classify_growth_trend,
    classify_metric_status,
    detect_status_changes,
    monitor_threshold_proximity,
    track_status_durations,
)

# Time Series primitives
from .time_series import (
    calculate_average_growth,
    calculate_cumulative_growth,
    calculate_pop_growth,
    calculate_rolling_averages,
    calculate_slope_of_time_series,
    calculate_to_date_growth_rates,
    convert_grain_to_freq,
)

# Trend Analysis primitives
from .trend_analysis import (
    analyze_metric_trend,
    calculate_benchmark_comparisons,
    detect_performance_plateau,
    detect_record_high,
    detect_record_low,
    detect_seasonality_pattern,
    detect_trend_exceptions,
    process_control_analysis,
)

# Create a dictionary of primitives organized by family
_primitive_families = {
    "numeric": [
        calculate_difference,
        calculate_percentage_difference,
        safe_divide,
        round_to_precision,
        calculate_gap_to_target,
    ],
    "performance": [
        calculate_metric_gva,
        calculate_historical_gva,
        classify_metric_status,
        detect_status_changes,
        track_status_durations,
        monitor_threshold_proximity,
        calculate_required_growth,
        classify_growth_trend,
        calculate_moving_target,
    ],
    "time_series": [
        calculate_average_growth,
        calculate_cumulative_growth,
        calculate_pop_growth,
        calculate_rolling_averages,
        calculate_to_date_growth_rates,
        convert_grain_to_freq,
        calculate_slope_of_time_series,
    ],
    "trend_analysis": [
        analyze_metric_trend,
        detect_performance_plateau,
        detect_record_high,
        detect_record_low,
        detect_trend_exceptions,
        process_control_analysis,
        detect_seasonality_pattern,
        calculate_benchmark_comparisons,
    ],
}


def list_primitives_by_family():
    """List all primitives organized by family"""
    result = {}
    for family, funcs in _primitive_families.items():
        result[family] = [func.__name__ for func in funcs]
    return result


def get_primitive_metadata(primitive_name: str):
    """Get metadata for a specific primitive"""
    # Find the primitive function by name
    primitive_func = globals().get(primitive_name)

    if not primitive_func:
        raise PrimitiveError(
            "Primitive not found",
            primitive_name,
            {"code": "PRIMITIVE_NOT_FOUND"},
        )

    # Extract metadata from docstring
    docstring = primitive_func.__doc__ or ""
    lines = [line.strip() for line in docstring.split("\n") if line.strip()]

    # Get a better description - first non-empty line that's not a metadata tag
    description = ""
    for line in lines:
        if line and not any(
            line.startswith(tag) for tag in ["Family:", "Version:", "Args:", "Returns:", "Notes:", "Raises:"]
        ):
            description = line
            break

    metadata = {
        "name": primitive_name,
        "description": description,
        "family": "",
        "version": "",
    }

    # Extract family and version
    for line in lines:
        if line.startswith("Family:"):
            metadata["family"] = line.replace("Family:", "").strip()
        elif line.startswith("Version:"):
            metadata["version"] = line.replace("Version:", "").strip()

    return metadata


__all__ = [
    # Numeric primitives
    "calculate_difference",
    "calculate_percentage_difference",
    "safe_divide",
    "round_to_precision",
    "calculate_gap_to_target",
    # Performance primitives
    "calculate_metric_gva",
    "calculate_historical_gva",
    "classify_metric_status",
    "detect_status_changes",
    "track_status_durations",
    "monitor_threshold_proximity",
    "calculate_required_growth",
    "classify_growth_trend",
    "calculate_moving_target",
    # Time Series primitives
    "calculate_average_growth",
    "calculate_cumulative_growth",
    "calculate_pop_growth",
    "calculate_rolling_averages",
    "calculate_to_date_growth_rates",
    "convert_grain_to_freq",
    "calculate_slope_of_time_series",
    # Trend Analysis primitives
    "analyze_metric_trend",
    "detect_performance_plateau",
    "detect_record_high",
    "detect_record_low",
    "detect_trend_exceptions",
    "process_control_analysis",
    "detect_seasonality_pattern",
    "calculate_benchmark_comparisons",
    # Utility functions
    "list_primitives_by_family",
    "get_primitive_metadata",
]
