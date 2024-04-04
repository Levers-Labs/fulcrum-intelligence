import pandas as pd


def describe_old(data: pd.DataFrame) -> list[dict]:
    """
    Describe the data

    param:
        data: pd.DataFrame.

    return:
        List[dict]: Statistics of metric values for each (dimension, slice) for a given metric ID, and
        a list of dimensions.

    example usage:

        analysis_manager = AnalysisManager()

        start_date = "2024-01-01"
        end_date = "2024-04-30"
        metric_id = "5"
        dimensions = ["Probability to Close Tier", "Owning Org", "Creating Org"]

        start_date = pd.to_datetime(start_date, format='%Y-%m-%d')
        end_date = pd.to_datetime(end_date, format='%Y-%m-%d')
        data['DAY'] = pd.to_datetime(data['DAY'], format='%Y-%m-%d')
        data['METRIC_ID'] = data['METRIC_ID'].astype(str)

        # Boolean indexing to filter rows
        data = data[(data['METRIC_ID'] == metric_id) &
                    (data['DAY'] >= start_date) &
                    (data['DAY'] <= end_date) &
                    (data['DIMENSION_NAME'].isin(dimensions))]

        response = analysis_manager.describe(data)

    sample response:
        [{'DIMENSION_NAME': 'Creating Org', 'SLICE': 'NA Sales', 'mean': 4.0, 'std': nan, 'p25': 4.0, 'p50': 4.0,
         'p75': 4.0, 'p90': 4.0, 'p95': 4.0, 'p99': 4.0, 'min': 4.0, 'max': 4.0, 'variance': nan, 'count': 1.0,
         'sum': 4.0, 'unique': 1},

        {'DIMENSION_NAME': 'Creating Org', 'SLICE': 'Other', 'mean': 50.0, 'std': nan, 'p25': 50.0, 'p50': 50.0,
         'p75': 50.0, 'p90': 50.0, 'p95': 50.0, 'p99': 50.0, 'min': 50.0, 'max': 50.0, 'variance': nan,
         'count': 1.0, 'sum': 50.0, 'unique': 1},

        {'DIMENSION_NAME': 'Owning Org', 'SLICE': 'NA CSM', 'mean': 56.0, 'std': nan, 'p25': 56.0, 'p50': 56.0,
         'p75': 56.0, 'p90': 56.0, 'p95': 56.0, 'p99': 56.0, 'min': 56.0, 'max': 56.0, 'variance': nan,
         'count': 1.0, 'sum': 56.0, 'unique': 1}
        ]
    """

    grouped_data = data.groupby(["DIMENSION_NAME", "SLICE"])
    metric_id = data["METRIC_ID"].iloc[0]
    # Calculate additional percentiles and variance for each group
    grouped_stats = grouped_data["METRIC_VALUE"].describe(percentiles=[0.25, 0.50, 0.75, 0.90, 0.95, 0.99])
    grouped_stats["variance"] = grouped_data["METRIC_VALUE"].var()
    grouped_stats["sum"] = grouped_data["METRIC_VALUE"].sum()
    grouped_stats["unique"] = grouped_data["METRIC_VALUE"].unique()

    grouped_stats.index.names = ["DIMENSION_NAME", "SLICE"]

    result = []

    # Iterate through the grouped DataFrame
    for index, row in grouped_stats.iterrows():
        stats_dict = {
            "metric_id": metric_id,
            "dimension": index[0],  # type: ignore
            "slice": index[1],  # type: ignore
            "mean": row["mean"],
            "median": row["50%"],
            "standard_deviation": row["std"],
            "percentile_25": row["25%"],
            "percentile_50": row["50%"],
            "percentile_75": row["75%"],
            "percentile_90": row["90%"],
            "percentile_95": row["95%"],
            "percentile_99": row["99%"],
            "min": row["min"],
            "max": row["max"],
            "variance": row["variance"],
            "count": row["count"],
            "sum": row["sum"],
            "unique": len(row["unique"]),
        }
        result.append(stats_dict)

    return result


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
                "metric_id": metric_id,
                "dimension": dimension,
                "slice": dimension_slice,
                "mean": filtered_df["value"].mean(),
                "median": all_percentiles.loc[0.50],
                "percentile_25": all_percentiles.loc[0.25],
                "percentile_50": all_percentiles.loc[0.50],
                "percentile_75": all_percentiles.loc[0.75],
                "percentile_90": all_percentiles.loc[0.90],
                "percentile_95": all_percentiles.loc[0.95],
                "percentile_99": all_percentiles.loc[0.99],
                "min": filtered_df["value"].min(),
                "max": filtered_df["value"].max(),
                "variance": filtered_df["value"].var(),
                "count": filtered_df["value"].count(),
                "sum": filtered_df["value"].sum(),
                "unique": len(filtered_df["value"].unique()),
            }
            # print(result)
            response.append(result)
    return response
