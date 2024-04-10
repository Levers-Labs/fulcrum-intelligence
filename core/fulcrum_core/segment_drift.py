import json
from datetime import datetime

import polars as pl

from dsensei.backend.app.insight.services.insight_builders import DFBasedInsightBuilder
from dsensei.backend.app.insight.api import InsightApi
from dsensei.backend.app.insight.services.utils import load_df_from_csv


def segment_drift(
    data: dict,
    debug: bool = False
):
    timestamp_format = "%Y-%m-%dT%H:%M:%S.%fZ"

    data["baseDateRange"]["from"] = datetime.strptime(data["baseDateRange"]["from"], "%Y-%m-%d").strftime(
        timestamp_format
    )
    data["baseDateRange"]["to"] = datetime.strptime(data["baseDateRange"]["to"], "%Y-%m-%d").strftime(timestamp_format)
    data["comparisonDateRange"]["from"] = datetime.strptime(data["comparisonDateRange"]["from"], "%Y-%m-%d").strftime(
        timestamp_format
    )
    data["comparisonDateRange"]["to"] = datetime.strptime(data["comparisonDateRange"]["to"], "%Y-%m-%d").strftime(
        timestamp_format
    )

    data_file_path = data["data_file_path"]
    expected_value = data["expectedValue"]
    (
        baseline_start,
        baseline_end,
        comparison_start,
        comparison_end,
        date_column,
        date_column_type,
        group_by_columns,
        filters,
        max_num_dimensions,
    ) = InsightApi.parse_data(data)

    metric_column = data["metricColumn"]
    aggregation_option = data["metricColumn"]["aggregationOption"]
    metric = InsightApi.parse_metrics(metric_column)

    df = load_df_from_csv(data_file_path, date_column).with_columns(
        pl.col(date_column).cast(pl.Utf8).str.slice(0, 10).str.to_date(strict=False).alias("date")
    )

    insight_builder = DFBasedInsightBuilder(
        df,
        (baseline_start, baseline_end),
        (comparison_start, comparison_end),
        group_by_columns,
        [metric],
        expected_value,
        filters,
        max_num_dimensions,
    )

    result = json.loads(insight_builder.build())
    main_key = f"{metric_column['singularMetric']['columnName']}_{aggregation_option.upper()}"
    dimension_slice_info = result[main_key]["dimensionSliceInfo"]
    overall_change = (result[main_key]["comparisonValue"] - result[main_key]["baselineValue"]) / result[main_key][
        "baselineValue"
    ]

    target_metric_direction = data["target_metric_direction"]

    for segment in dimension_slice_info.keys():
        try:
            dimensionslice_comparison_slicevalue = dimension_slice_info[segment]["comparisonValue"]["sliceValue"]
        except KeyError:
            dimensionslice_comparison_slicevalue = 0

        try:
            dimensionslice_baseline_slicevalue = dimension_slice_info[segment]["baselineValue"]["sliceValue"]
        except KeyError:
            dimensionslice_baseline_slicevalue = 0

        change = (
            dimensionslice_comparison_slicevalue - dimensionslice_baseline_slicevalue
        ) / dimensionslice_baseline_slicevalue

        relative_change = (change - overall_change) * 100
        dimension_slice_info[segment]["relative_change"] = relative_change

        if (relative_change > 0 and target_metric_direction == "increasing") or (
            relative_change < 0 and target_metric_direction == "decreasing"
        ):
            dimension_slice_info[segment]["pressure"] = "UPWARD"
        elif (relative_change > 0 and target_metric_direction == "decreasing") or (
            relative_change < 0 and target_metric_direction == "increasing"
        ):
            dimension_slice_info[segment]["pressure"] = "DOWNWARD"
        elif relative_change == 0.0:
            dimension_slice_info[segment]["pressure"] = "UNCHANGED"

    if debug:
        return result

    return dimension_slice_info

