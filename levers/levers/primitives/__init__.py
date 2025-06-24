# primitives/__init__.py
# Import and expose all primitives for easy access

# Numeric primitives
from levers.exceptions import PrimitiveError

from .numeric import (
    calculate_difference,
    calculate_gap_to_target,
    calculate_percentage_difference,
    calculate_relative_change,
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

# Period Grains primitives
from .period_grains import (
    get_date_range_from_window,
    get_period_length_for_grain,
    get_period_range_for_grain,
    get_prev_period_start_date,
    get_prior_period_range,
    get_period_end_date,
)

# Time Series primitives
from .time_series import (
    calculate_average_growth,
    calculate_benchmark_comparisons,
    calculate_cumulative_growth,
    calculate_pop_growth,
    calculate_rolling_averages,
    calculate_slope_of_time_series,
    convert_grain_to_freq,
    validate_date_sorted,
)

# Trend Analysis primitives
from .trend_analysis import (
    analyze_metric_trend,
    detect_performance_plateau,
    detect_record_high,
    detect_record_low,
    detect_seasonality_pattern,
    detect_trend_exceptions,
    process_control_analysis,
    detect_trend_exceptions_using_spc_analysis,
    analyze_trend_using_spc_analysis,
)

# Dimensional Analysis primitives
from .dimensional_analysis import (
    analyze_composition_changes,
    analyze_dimension_impact,
    build_slices_performance_list,
    calculate_concentration_index,
    calculate_slice_metrics,
    compare_dimension_slices_over_time,
    compute_historical_slice_rankings,
    compute_slice_shares,
    compute_top_bottom_slices,
    difference_from_average,
    highlight_slice_comparisons,
    identify_largest_smallest_by_share,
    identify_strongest_weakest_changes,
    rank_metric_slices,
)

# Forecasting primitives
from .forecasting import (
    simple_forecast,
    forecast_with_confidence_intervals,
    calculate_forecast_accuracy,
    generate_forecast_scenarios,
)

# Create a dictionary of primitives organized by family
_primitive_families = {
    "numeric": [
        calculate_difference,
        calculate_percentage_difference,
        calculate_relative_change,
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
    "period_grains": [
        get_period_range_for_grain,
        get_prior_period_range,
        get_prev_period_start_date,
        get_date_range_from_window,
        get_period_length_for_grain,
        get_period_end_date,
    ],
    "time_series": [
        calculate_average_growth,
        calculate_benchmark_comparisons,
        calculate_cumulative_growth,
        calculate_pop_growth,
        calculate_rolling_averages,
        convert_grain_to_freq,
        validate_date_sorted,
    ],
    "trend_analysis": [
        analyze_metric_trend,
        detect_performance_plateau,
        detect_record_high,
        detect_record_low,
        detect_trend_exceptions,
        process_control_analysis,
        detect_seasonality_pattern,
        detect_trend_exceptions_using_spc_analysis,
        analyze_trend_using_spc_analysis,
    ],
    "dimensional_analysis": [
        # Slice Metrics & Shares
        calculate_slice_metrics,
        compute_slice_shares,
        rank_metric_slices,
        # Composition & Impact Analysis
        analyze_composition_changes,
        analyze_dimension_impact,
        calculate_concentration_index,
        # Time Comparison
        compare_dimension_slices_over_time,
        # Comparative Analysis
        difference_from_average,
        compute_top_bottom_slices,
        identify_largest_smallest_by_share,
        identify_strongest_weakest_changes,
        highlight_slice_comparisons,
        compute_historical_slice_rankings,
        build_slices_performance_list,
    ],
    "forecasting": [
        simple_forecast,
        forecast_with_confidence_intervals,
        calculate_forecast_accuracy,
        generate_forecast_scenarios,
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
    "calculate_relative_change",
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
    # Period Grains primitives
    "get_period_range_for_grain",
    "get_prior_period_range",
    "get_prev_period_start_date",
    "get_date_range_from_window",
    "get_period_end_date",
    # Time Series primitives
    "calculate_average_growth",
    "calculate_benchmark_comparisons",
    "calculate_cumulative_growth",
    "calculate_pop_growth",
    "calculate_rolling_averages",
    "convert_grain_to_freq",
    "calculate_slope_of_time_series",
    "validate_date_sorted",
    # Trend Analysis primitives
    "analyze_metric_trend",
    "detect_performance_plateau",
    "detect_record_high",
    "detect_record_low",
    "detect_trend_exceptions",
    "process_control_analysis",
    "detect_seasonality_pattern",
    "detect_trend_exceptions_using_spc_analysis",
    "analyze_trend_using_spc_analysis",
    # Dimensional Analysis primitives
    "calculate_slice_metrics",
    "compute_slice_shares",
    "rank_metric_slices",
    "analyze_composition_changes",
    "analyze_dimension_impact",
    "calculate_concentration_index",
    "compare_dimension_slices_over_time",
    "difference_from_average",
    "compute_top_bottom_slices",
    "identify_largest_smallest_by_share",
    "identify_strongest_weakest_changes",
    "highlight_slice_comparisons",
    "compute_historical_slice_rankings",
    "build_slices_performance_list",
    # Forecasting primitives,
    "simple_forecast",
    "forecast_with_confidence_intervals",
    "calculate_forecast_accuracy",
    "generate_forecast_scenarios",
    # Utility functions
    "list_primitives_by_family",
    "get_primitive_metadata",
]
