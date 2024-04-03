import logging
from datetime import date
from itertools import combinations
from typing import Tuple, List

import numpy as np
import pandas as pd
import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class AnalysisManager:
    """
    Core class for implementing all major functions for analysis manager
    """

    def describe(self, data: pd.DataFrame) -> list[dict]:
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

    def correlate(self, data: pd.DataFrame, start_date: date, end_date: date) -> list[dict]:
        """
        Compute the correlation between all the nC2 pairs generated from the given list metric_ids.

        param:
            data: pd.DataFrame. Already filtered on metric_ids, start_date, and end_date.
            start_date: pandas timestamp formatted in YYYY-MM-DD
            end_date: pandas timestamp formatted in YYYY-MM-DD

        return:
            dict having a list of correlation_coefficient, start_date, and end_date:
            [
              {
                "metric_id_1": "5",
                "metric_id_2": "6",
                "start_date": "2024-01-01",
                "end_date": "2024-04-30",
                "correlation_coefficient": 0.8
              },
              {
                "metric_id_1": "5",
                "metric_id_2": "7",
                "start_date": "2024-01-01",
                "end_date": "2024-04-30",
                "correlation_coefficient": 0.9
              }
            ]

        example usage:

            analysis_manager = AnalysisManager()

            start_date = "2020-01-01"
            end_date = "2025-04-30"

            start_date = pd.to_datetime(start_date, format="%Y-%m-%d")
            end_date = pd.to_datetime(end_date, format="%Y-%m-%d")
            data['DAY'] = pd.to_datetime(data['DAY'], format='%Y-%m-%d')
            data['METRIC_ID'] = data['METRIC_ID'].astype(str)


            data = data[(data["DAY"] >= start_date) & (data["DAY"] <= end_date)]

            response = analysis_manager.correlate(data, [], start_date=start_date, end_date=end_date)
        """
        data["METRIC_VALUE"] = pd.to_numeric(data["METRIC_VALUE"], errors="coerce")

        data = data.drop_duplicates(subset=["DAY", "METRIC_ID"]).reset_index(drop=True)

        precomputed_metric_values = {}
        for metric_id, group in data.groupby("METRIC_ID"):
            precomputed_metric_values[metric_id] = group[["METRIC_VALUE", "DAY"]]

        response = []

        for metric_id1, metric_id2 in combinations(precomputed_metric_values.keys(), 2):
            df1 = precomputed_metric_values[metric_id1]
            df2 = precomputed_metric_values[metric_id2]

            df1.drop_duplicates(subset=["DAY"]).reset_index(drop=True)
            df2.drop_duplicates(subset=["DAY"]).reset_index(drop=True)

            df1.dropna(inplace=True)
            df2.dropna(inplace=True)

            common_days = set(df1["DAY"]).intersection(df2["DAY"])

            df1 = df1[df1["DAY"].isin(common_days)]
            df2 = df2[df2["DAY"].isin(common_days)]

            series1 = df1["METRIC_VALUE"]
            series2 = df2["METRIC_VALUE"]

            correlation = np.corrcoef(series1, series2)[0, 1]
            response.append(
                {
                    "metric_id_1": metric_id1,
                    "metric_id_2": metric_id2,
                    "correlation_coefficient": correlation,
                    "start_date": start_date,
                    "end_date": end_date,
                }
            )

        return response

    def _compute_initial_half_averages(
        self,
        data,
        index_of_first_half_average,
        processing_start_index,
        half_average_point,
        latest_two_half_average_indices,
    ):
        current_half_average_index = index_of_first_half_average
        row_count = 0
        half_average = 0
        initial_half_average_calculation_frequency = 2
        for _, row in data[processing_start_index:].iterrows():
            half_average += row["METRIC_VALUE"]
            row_count += 1

            if row_count % half_average_point == 0:
                latest_two_half_average_indices.append(int(current_half_average_index))
                data.at[current_half_average_index, "HALF_AVERAGE"] = half_average / half_average_point
                data.at[current_half_average_index, "CENTRAL_LINE"] = half_average / half_average_point
                current_half_average_index += half_average_point
                half_average = 0
                initial_half_average_calculation_frequency -= 1

            if initial_half_average_calculation_frequency == 0:
                break
        return data

    def _set_increment_per_time_period(
        self, data, processing_start_index, latest_two_half_average_indices, half_average_point
    ):

        current_increment_per_time_period_index = processing_start_index
        data.at[current_increment_per_time_period_index, "INCREMENT_PER_TIME_PERIOD"] = (
            data.at[latest_two_half_average_indices[1], "HALF_AVERAGE"]
            - data.at[latest_two_half_average_indices[0], "HALF_AVERAGE"]
        ) / half_average_point

        return data, current_increment_per_time_period_index

    def _compute_central_line(
        self,
        data,
        latest_two_half_average_indices,
        half_average_point,
        processing_start_index,
        current_increment_per_time_period_index,
    ):
        # set the values at the indices of half average
        data.at[latest_two_half_average_indices[0], "CENTRAL_LINE"] = data.at[
            latest_two_half_average_indices[0], "HALF_AVERAGE"
        ]
        data.at[latest_two_half_average_indices[1], "CENTRAL_LINE"] = data.at[
            latest_two_half_average_indices[1], "HALF_AVERAGE"
        ]

        # set the first value:
        data.at[processing_start_index, "CENTRAL_LINE"] = data.at[
            latest_two_half_average_indices[0], "CENTRAL_LINE"
        ] - ((half_average_point // 2) * data.at[current_increment_per_time_period_index, "INCREMENT_PER_TIME_PERIOD"])

        for processing_start_index_rolling, _ in data[processing_start_index:].iterrows():
            if processing_start_index_rolling == processing_start_index:
                continue

            data.at[processing_start_index_rolling, "CENTRAL_LINE"] = (
                data.at[current_increment_per_time_period_index, "INCREMENT_PER_TIME_PERIOD"]
                # ruff-ignore[call-overload]
                + data.at[processing_start_index_rolling - 1, "CENTRAL_LINE"]  # type: ignore
            )
        return data

    def _compute_average_moving_ranges(self, data, processing_start_index, half_average_point, debug):
        average_moving_range: float = 0.0
        average_moving_range_row_count = 0
        for processing_start_index_rolling, _ in data[processing_start_index:].iterrows():
            if processing_start_index_rolling == 0:
                continue
            average_moving_range += data.at[processing_start_index_rolling, "MOVING_RANGES"]
            average_moving_range_row_count += 1

            if average_moving_range_row_count == (half_average_point + 1):
                if debug:
                    logger.debug(
                        "average_moving_range: " + str(average_moving_range) + " " + str(half_average_point + 1),
                    )
                average_moving_range = float(average_moving_range) / float(half_average_point + 1)
                break

        data["AVERAGE_MOVING_RANGE"][processing_start_index:] = average_moving_range

        return data

    def _compute_limits(self, data, processing_start_index):
        data["LCL"][processing_start_index:] = data.apply(
            lambda row: max(row["CENTRAL_LINE"] - row["AVERAGE_MOVING_RANGE"] * 2.66, 0),
            axis=1,
        )[processing_start_index:]
        data["UCL"][processing_start_index:] = data.apply(
            lambda row: row["CENTRAL_LINE"] + row["AVERAGE_MOVING_RANGE"] * 2.66, axis=1
        )[processing_start_index:]

        return data

    def _detect_signal(self, data, processing_start_index):

        consecutive_above_central_line_count = 0
        consecutive_below_central_line_count = 0
        values_above_or_below_central_line = []
        closer_to_ucl_or_lcl_than_central_line_count = []
        is_signal_detected = False
        signal_detected_index = -1

        for processing_start_index_rolling, _ in data[processing_start_index:].iterrows():

            if (
                data.at[processing_start_index_rolling, "METRIC_VALUE"] > data.at[processing_start_index_rolling, "UCL"]
                or data.at[processing_start_index_rolling, "METRIC_VALUE"]
                < data.at[processing_start_index_rolling, "LCL"]
            ):
                # SIGNAL DETECTED
                # ruff-ignore[call-overload]
                signal_detected_index = int(processing_start_index_rolling)  # type: ignore
                data.at[processing_start_index_rolling, "SIGNAL_DETECTED"] = "rule_1"
                is_signal_detected = True
                break

            if data.at[processing_start_index_rolling, "METRIC_VALUE"] > data.at[processing_start_index_rolling, "UCL"]:
                consecutive_above_central_line_count += 1
                consecutive_below_central_line_count = 0

            if data.at[processing_start_index_rolling, "METRIC_VALUE"] < data.at[processing_start_index_rolling, "LCL"]:
                consecutive_below_central_line_count += 1
                consecutive_above_central_line_count = 0

            if consecutive_above_central_line_count == 7 or consecutive_below_central_line_count == 7:
                # SIGNAL DETECTED
                # ruff-ignore[call-overload]
                signal_detected_index = int(processing_start_index_rolling)  # type: ignore
                data.at[processing_start_index_rolling, "SIGNAL_DETECTED"] = "rule_2"
                is_signal_detected = True
                break

            if abs(
                data.at[processing_start_index_rolling, "METRIC_VALUE"] - data.at[processing_start_index_rolling, "UCL"]
            ) < abs(
                data.at[processing_start_index_rolling, "METRIC_VALUE"]
                - data.at[processing_start_index_rolling, "CENTRAL_LINE"]
            ) or abs(
                data.at[processing_start_index_rolling, "METRIC_VALUE"] - data.at[processing_start_index_rolling, "LCL"]
            ) < abs(
                data.at[processing_start_index_rolling, "METRIC_VALUE"]
                - data.at[processing_start_index_rolling, "CENTRAL_LINE"]
            ):
                closer_to_ucl_or_lcl_than_central_line_count.append("y")
            else:
                closer_to_ucl_or_lcl_than_central_line_count.append("n")

            # check the last 4 values in the array closer_to_ucl_or_lcl_than_central_line_count:
            if len(closer_to_ucl_or_lcl_than_central_line_count) >= 4:
                if closer_to_ucl_or_lcl_than_central_line_count[-4:].count("y") >= 3:
                    # SIGNAL DETECTED
                    # ruff-ignore[call-overload]
                    signal_detected_index = int(processing_start_index_rolling)  # type: ignore
                    data.at[processing_start_index_rolling, "SIGNAL_DETECTED"] = "rule_3"
                    is_signal_detected = True
                    break

            if (
                data.at[processing_start_index_rolling, "METRIC_VALUE"]
                > data.at[processing_start_index_rolling, "CENTRAL_LINE"]
            ):
                values_above_or_below_central_line.append("a")
            elif (
                data.at[processing_start_index_rolling, "METRIC_VALUE"]
                < data.at[processing_start_index_rolling, "CENTRAL_LINE"]
            ):
                values_above_or_below_central_line.append("b")

            # check if 10 out of last 12 values - all are above or below central line
            if (
                values_above_or_below_central_line[-12:].count("a") >= 10
                or values_above_or_below_central_line[-12:].count("b") >= 10
            ):
                # SIGNAL DETECTED
                # ruff-ignore[call-overload]
                signal_detected_index = int(processing_start_index_rolling)  # type: ignore
                data.at[processing_start_index_rolling, "SIGNAL_DETECTED"] = "rule_4"
                is_signal_detected = True
                break
        return data, is_signal_detected, signal_detected_index

    def _apply_process_control_from_index(
        self,
        data: pd.DataFrame,
        half_average_point: int,
        processing_start_index: int,
        debug: bool = False,
    ) -> Tuple[int, pd.DataFrame]:

        signal_detected_index = -1

        # case when there are not enough points left in the dataframe
        if processing_start_index + half_average_point + 1 > len(data) - 1:
            return signal_detected_index, data

        index_of_first_half_average = processing_start_index + half_average_point // 2
        latest_two_half_average_indices: List[float] = []

        # compute initial half averages
        data = self._compute_initial_half_averages(
            data,
            index_of_first_half_average,
            processing_start_index,
            half_average_point,
            latest_two_half_average_indices,
        )

        if debug:
            logger.debug(latest_two_half_average_indices)

        if len(latest_two_half_average_indices) < 2:
            return signal_detected_index, data

        # setting the INCREMENT_PER_TIME_PERIOD
        data, current_increment_per_time_period_index = self._set_increment_per_time_period(
            data, processing_start_index, latest_two_half_average_indices, half_average_point
        )

        # compute entire central line
        data = self._compute_central_line(
            data,
            latest_two_half_average_indices,
            half_average_point,
            processing_start_index,
            current_increment_per_time_period_index,
        )

        # compute the average moving ranges
        data = self._compute_average_moving_ranges(data, processing_start_index, half_average_point, debug)

        # compute the UCL & LCL
        data = self._compute_limits(data, processing_start_index)

        # now start scanning for signal
        data, is_signal_detected, signal_detected_index = self._detect_signal(data, processing_start_index)

        if debug:
            signal_detected_index = 22

        if not is_signal_detected:
            signal_detected_index = -1

        return signal_detected_index, data

    def process_control(
        self,
        data: pd.DataFrame,
        metric_id: str,
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
        grain: str,
        debug: bool = False,
    ) -> dict:
        """
        Implement the process control for the given list of metric values.

        param:
            metric_id: given metric_id in string format
            start_date = pd.to_datetime(start_date, format="%Y-%m-%d")
            end_date = pd.to_datetime(end_date, format="%Y-%m-%d")
            grain: Can be "DAY", "WEEK", "MONTH", and "QUARTER".

        NOTES:
             1. For any grain, there should be a 1:1 mapping between the grain and the metric value.
             2. Before passing the data to this function, it should be filtered on metric_id, start_date, and end_date.
             For e.g.: please have a look the file core/data/process_control.csv to check how the data should look like.
             3. Data should be in the sorted order of GRAIN used.

        response:
            A dict having:
                1. Half averages: numpy array
                2. Central Line: numpy array
                3. UCL: numpy array
                4. LCL: numpy array
                (additional information on the above-mentioned columns mentioned
                 in the blog - https://www.staceybarr.com/measure-up/interpreting-signals-in-trending-kpis/)
                5. date: string - value of the 1st row of the column 'GRAIN'
                6. grain: string - the grain on which we want to apply the process control. Can be DAY, WEEK, MONTH,
                    and QUARTER
                7. start_date: string - the start date from the data has been filtered
                8. end_date: string - the end date from the data has been filtered
                9. metric_id: string - the metric_id on which the data has been filtered

        Example usage:

            from core.fulcrum_core import AnalysisManager
            analysis_manager = AnalysisManager()
            response = analysis_manager.process_control(data, "","","","MONTH" , debug=False)
            logger.debug("Test file response:\n", response)

        """
        data.index = data.index.astype(int)
        data["METRIC_VALUE"] = pd.to_numeric(data["METRIC_VALUE"], errors="coerce")
        data = data.drop_duplicates(subset=["GRAIN"]).reset_index(drop=True)  # ensuring 1:1 mapping

        data["HALF_AVERAGE"] = np.nan
        data["INCREMENT_PER_TIME_PERIOD"] = np.nan
        data["CENTRAL_LINE"] = np.nan
        half_average_point = 9
        data["AVERAGE_MOVING_RANGE"] = np.nan
        data["UCL"] = np.nan
        data["LCL"] = np.nan
        data["SIGNAL_DETECTED"] = ""

        # compute the moving ranges
        data["MOVING_RANGES"] = data["METRIC_VALUE"].diff().abs()

        signal_detected_index = 0

        signal_detected_index, data = self._apply_process_control_from_index(
            data, half_average_point, signal_detected_index, debug=debug
        )
        while signal_detected_index != -1:
            signal_detected_index, data = self._apply_process_control_from_index(
                data, half_average_point, signal_detected_index, debug=debug
            )
            if debug:
                logger.debug("Signal detected index: " + str(signal_detected_index))
                break

        if debug:
            logger.debug(data)
        return {
            "metric_id": metric_id,
            "start_date": start_date,
            "end_date": end_date,
            "grain": grain,
            "date": data.at[0, "GRAIN"],
            "half_average": data["HALF_AVERAGE"].tolist(),
            "central_line": data["CENTRAL_LINE"].tolist(),
            "ucl": data["UCL"].tolist(),
            "lcl": data["LCL"].tolist(),
        }
