import pandas as pd


def describe(
    data: list[dict],
    dimensions: list[str],
    metric_id: str,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    aggregation_function: str,
) -> list[dict]:
    """
    Describe the data

    param:
        data: List[dict]. Example can be found out in core/tests/data/describe_data.json
        dimensions: List['str']. For example: ["region", "stage_name"]
        metric_id: str. Example: "ToMRR"
        start_date: pd.Timestamp
        end_date: pd.Timestamp

    return:
        List[dict]: Statistics of metric values for each (dimension, slice) for a given metric ID within the given start
        and end date ranges.

    example usage:
        with open('data/describe_data.json', 'r') as fr:
            describe_data = json.load(fr)

        from core.fulcrum_core import AnalysisManager

        analysis_manager = AnalysisManager()

        start_date = pd.to_datetime("2023-01-01")
        end_date = pd.to_datetime("2025-01-01")
        metric_id = "ToMRR"
        dimensions = ["region", "stage_name"]

        result = analysis_manager.describe(describe_data, dimensions, metric_id, start_date=start_date,
         end_date=end_date,aggregation_function='sum')

        print(result)


    sample response:
        [
            {'metric_id': 'ToMRR', 'dimension': 'region', 'slice': 'Asia', 'mean': 1050.0, 'median': 1050.0,
             'percentile_25': 775.0, 'percentile_50': 1050.0, 'percentile_75': 1325.0, 'percentile_90': 1490.0,
              'percentile_95': 1545.0, 'percentile_99': 1589.0, 'min': 500, 'max': 1600, 'variance': 605000.0,
               'count': 2, 'sum': 2100, 'unique': 2},
            {'metric_id': 'ToMRR', 'dimension': 'region', 'slice': 'EMEA', 'mean': 433.3333333333333, 'median': 300.0,
             'percentile_25': 250.0, 'percentile_50': 300.0, 'percentile_75': 550.0, 'percentile_90': 700.0,
              'percentile_95': 750.0, 'percentile_99': 790.0, 'min': 200, 'max': 800, 'variance': 103333.33333333334,
               'count': 3, 'sum': 1300, 'unique': 3},
            {'metric_id': 'ToMRR', 'dimension': 'stage_name', 'slice': 'Won',
                'mean': 850.0, 'median': 850.0, 'percentile_25': 675.0, 'percentile_50': 850.0, 'percentile_75': 1025.0,
                 'percentile_90': 1130.0, 'percentile_95': 1165.0, 'percentile_99': 1193.0, 'min': 500, 'max': 1200,
                  'variance': 245000.0, 'count': 2, 'sum': 1700, 'unique': 2},
            {'metric_id': 'ToMRR', 'dimension': 'stage_name', 'slice': 'Lost', 'mean': 300.0, 'median': 300.0,
             'percentile_25': 300.0, 'percentile_50': 300.0, 'percentile_75': 300.0, 'percentile_90': 300.0,
              'percentile_95': 300.0, 'percentile_99': 300.0, 'min': 300, 'max': 300, 'variance': nan, 'count': 1,
               'sum': 300, 'unique': 1},
            {'metric_id': 'ToMRR', 'dimension': 'stage_name', 'slice': 'Procurement', 'mean': 400.0, 'median': 400.0,
             'percentile_25': 400.0, 'percentile_50': 400.0, 'percentile_75': 400.0, 'percentile_90': 400.0,
              'percentile_95': 400.0, 'percentile_99': 400.0, 'min': 400, 'max': 400, 'variance': nan, 'count': 1,
               'sum': 400, 'unique': 1},
            {'metric_id': 'ToMRR', 'dimension': 'stage_name', 'slice': 'Sale', 'mean': 500.0, 'median': 500.0,
             'percentile_25': 350.0, 'percentile_50': 500.0, 'percentile_75': 650.0, 'percentile_90': 740.0,
              'percentile_95': 770.0, 'percentile_99': 794.0, 'min': 200, 'max': 800, 'variance': 180000.0,
               'count': 2, 'sum': 1000, 'unique': 2}
        ]

    """
    # convert list into df
    df = pd.DataFrame(data=data)
    df["date"] = pd.to_datetime(df["date"])
    response = []
    for dimension in dimensions:
        # group by on that dimension, date, metric_id
        grouped_df = df.groupby([dimension, "date", "metric_id"]).agg({"value": aggregation_function}).reset_index()
        # print("Grouped df:\n", grouped_df)
        unique_slices_for_dimension = set(grouped_df[dimension])
        # print("Slices: ", unique_slices_for_dimension)
        for dimension_slice in unique_slices_for_dimension:
            filtered_df = grouped_df.loc[
                (grouped_df[dimension] == dimension_slice)
                & (grouped_df["date"] >= start_date)
                & (grouped_df["date"] <= end_date)
            ]
            # print("Filtered df:\n", filtered_df)
            all_percentiles = filtered_df["value"].quantile([0.25, 0.50, 0.75, 0.90, 0.95, 0.99])
            # print("Percentiles: ", type(all_percentiles))
            result = {
                "metric_id": str(metric_id),
                "dimension": str(dimension),
                "member": str(dimension_slice),
                "mean": float(filtered_df["value"].mean()),
                "median": float(all_percentiles.loc[0.50]),
                "percentile_25": float(all_percentiles.loc[0.25]),
                "percentile_50": float(all_percentiles.loc[0.50]),
                "percentile_75": float(all_percentiles.loc[0.75]),
                "percentile_90": float(all_percentiles.loc[0.90]),
                "percentile_95": float(all_percentiles.loc[0.95]),
                "percentile_99": float(all_percentiles.loc[0.99]),
                "min": float(filtered_df["value"].min()),
                "max": float(filtered_df["value"].max()),
                "variance": float(filtered_df["value"].var()),
                "count": int(filtered_df["value"].count()),
                "sum": float(filtered_df["value"].sum()),
                "unique": int(len(filtered_df["value"].unique())),
            }
            for k, v in result.items():
                if pd.isna(v):  # type: ignore
                    result[k] = None

            # print(result)
            response.append(result)
    return response
