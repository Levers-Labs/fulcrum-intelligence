# primitives/__init__.py
# Import and expose all primitives for easy access

# Numeric primitives
from .numeric import (
    calculate_difference,
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

# And so on for other primitive families...

# Create a dictionary of primitives organized by family
_primitive_families = {
    "numeric": [
        calculate_difference,
        calculate_percentage_difference,
        safe_divide,
        round_to_precision,
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
    # Add other families here
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
        raise ValueError(f"Primitive '{primitive_name}' not found")

    # Extract metadata from docstring
    docstring = primitive_func.__doc__ or ""
    lines = docstring.split("\n")

    metadata = {
        "name": primitive_name,
        "description": lines[0].strip() if lines else "",
        "family": "",
        "version": "",
    }

    # Extract family and version
    for line in lines:
        line = line.strip()
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
    # Utility functions
    "list_primitives_by_family",
    "get_primitive_metadata",
]
