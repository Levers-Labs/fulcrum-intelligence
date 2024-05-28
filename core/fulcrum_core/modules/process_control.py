import logging
import math
from typing import Any

import numpy as np
import pandas as pd

from fulcrum_core.execptions import InsufficientDataError
from fulcrum_core.modules import BaseAnalyzer

logger = logging.getLogger(__name__)


class ProcessControlAnalyzer(BaseAnalyzer):
    """
    A class for analyzing time series data using process control techniques,
    incorporating dynamic trend line and control limit calculations.
    """

    HALF_AVERAGE_POINT = 9
    CONSECUTIVE_SIGNAL_PERIODS = 5
    MOVING_RANGE_MULTIPLIER = 2.66
    # rule constants,
    # e.g., 7 Individual Values in a row are above or below the Center Line
    CONSECUTIVE_RUN_LENGTH = 7
    # e.g., 10 out of 12 Individual Values are above or below the Center Line
    LONG_RUN_TOTAL_LENGTH = 12
    LONG_RUN_MIN_LENGTH = 10
    # e.g., 3 out of 4 Individual Values are closer to the UCL or LCL than the Center Line
    SHORT_RUN_TOTAL_LENGTH = 4
    SHORT_RUN_MIN_LENGTH = 3

    def get_half_average_point(self, start_index: int, end_index: int) -> int:
        """
        get the half-average point based on the specified range of data.

        Args:
            start_index (int): The starting index of the data range.
            end_index (int): The ending index of the data range.

        Returns:
            int: The half-average point.
        """
        half_average_point = self.HALF_AVERAGE_POINT
        # change half-average point if the number of data points is less than the default value
        if end_index - start_index < half_average_point * 2:
            half_average_point = (end_index - start_index) // 2
        return half_average_point

    def compute_half_averages(self, df: pd.DataFrame, start_index: int, end_index: int) -> list[float]:
        """
        compute the half-averages based on the specified range of data.

        Args:
            df (pd.DataFrame): The input time series data with 'date' and 'value' columns.
            start_index (int): The starting index of the data range.
            end_index (int): The ending index of the data range.

        Returns:
            list[float]: The computed half-averages.
        """
        half_average_point = self.get_half_average_point(start_index, end_index)
        # 2 averages needed, one till the half and one after the half
        first_half_average = df.iloc[start_index : start_index + half_average_point]["value"].mean()
        second_half_average = df.iloc[end_index - half_average_point : end_index]["value"].mean()
        logger.debug("Computed half-averages: %s, %s", first_half_average, second_half_average)
        return [first_half_average, second_half_average]

    def compute_central_line(
        self, half_averages: list[float], start_index: int, end_index: int, data_points: int
    ) -> tuple[list[float], float]:
        """
        compute the central line based on the half-averages.

        Args:
            half_averages (list[float]): The computed half-averages.
            start_index (int): The starting index of the data range.
            end_index (int): The ending index of the data range.
            data_points (int): The number of data points in the range.

        Returns:
            list[float]: The computed central line values and the slope of the central line.
        """

        slope = (half_averages[-1] - half_averages[0]) / self.HALF_AVERAGE_POINT
        logger.debug("Computed slope: %s", slope)
        half_average_point = self.get_half_average_point(start_index, end_index)
        central_line_start = start_index + half_average_point // 2
        central_line: list = [None] * data_points
        # Compute central line values for the first half-average
        central_line[central_line_start - start_index] = half_averages[0]
        # Compute central line values based on the increment per period
        for i in range(central_line_start - start_index + 1, data_points):
            central_line[i] = central_line[i - 1] + slope
        # Compute central line values for the previous data points
        for i in range(central_line_start - start_index - 1, -1, -1):
            central_line[i] = central_line[i + 1] - slope
        logger.debug("Computed central line: %s", central_line)
        return central_line, slope

    def compute_control_limits(self, df: pd.DataFrame, central_line: list[float]) -> tuple[list[float], list[float]]:
        """
        compute the upper and lower control limits based on the central line.

        Args:
            df (pd.DataFrame): The input time series data with 'date' and 'value' columns.
            central_line (list[float]): The computed central line values.

        Returns:
            Tuple[List[float], List[float]]: The upper control limits (UCL) and lower control limits (LCL).
        """
        moving_ranges = df["value"].diff().abs()
        average_moving_range = moving_ranges[1 : 2 * math.ceil(self.HALF_AVERAGE_POINT / 2) + 1].mean()
        ucl = [max(cl + average_moving_range * 2.66, 0) for cl in central_line]
        logger.debug("Computed UCL: %s", ucl)
        lcl = [max(cl - average_moving_range * 2.66, 0) for cl in central_line]
        logger.debug("Computed LCL: %s", lcl)
        return ucl, lcl

    def detect_signal(
        self, df: pd.DataFrame, central_line: list[float], ucl: list[float], lcl: list[float]
    ) -> list[int]:
        """
        detect signals based on the specified Wheeler rules.

        Args:
            df (pd.DataFrame): The input time series data with 'date' and 'value' columns.
            central_line (list[float]): The computed central line values for each point.
            ucl (list[float]): The computed upper control limits for each point.
            lcl (list[float]): The computed lower control limits for each point.

        Returns:
            list[int]: Indices for which a signal is detected.
        """
        df = df.copy()
        # Ensure dataframe has the required control limits and center line values for each data point
        df["central_line"] = central_line
        df["ucl"] = ucl
        df["lcl"] = lcl

        # Rule 1: An Individual Value is outside the Control Limits
        rule1 = (df["value"] > df["ucl"]) | (df["value"] < df["lcl"])

        # Rule 2: next 'n' individual Values in a row are above or below the Center Line
        indexer = pd.api.indexers.FixedForwardWindowIndexer(window_size=self.CONSECUTIVE_RUN_LENGTH)
        rule2 = (
            (df["value"] > df["central_line"]).rolling(window=indexer, min_periods=1).sum()
            == self.CONSECUTIVE_RUN_LENGTH
        ) | (
            (df["value"] < df["central_line"]).rolling(window=indexer, min_periods=1).sum()
            == self.CONSECUTIVE_RUN_LENGTH
        )

        # Rule 3: x out of x + n Individual Values are above or below the Center Line
        indexer = pd.api.indexers.FixedForwardWindowIndexer(window_size=self.LONG_RUN_TOTAL_LENGTH)
        rule3 = (
            (df["value"] > df["central_line"]).rolling(window=indexer, min_periods=1).sum() >= self.LONG_RUN_MIN_LENGTH
        ) | (
            (df["value"] < df["central_line"]).rolling(window=indexer, min_periods=1).sum() >= self.LONG_RUN_MIN_LENGTH
        )

        # Rule 4: x-n out of x Individual Values are closer to the UCL or LCL than the Center Line
        indexer = pd.api.indexers.FixedForwardWindowIndexer(window_size=self.SHORT_RUN_TOTAL_LENGTH)
        rule4 = (
            (df["value"] > (df["central_line"] + (df["ucl"] - df["central_line"]) / 2))
            | (df["value"] < (df["central_line"] - (df["central_line"] - df["lcl"]) / 2))
        ).rolling(window=indexer, min_periods=1).sum() >= self.SHORT_RUN_MIN_LENGTH

        # Combine the rules and get the indices where any rule is triggered
        signal_indices = df.index[(rule1 | rule2 | rule3 | rule4)].tolist()

        return signal_indices

    def is_recalculation_needed(self, signal_indices: list[int]) -> int | None:
        """
        if a signal is detected for at least self.CONSECUTIVE_SIGNAL_PERIODS(5) consecutive periods,
        the Central Line, UCL, and LCL are recalculated.

        e.g.
        signal_indices = [18, 24, 25, 26, 27, 28, 32, 38, 38, 16] then first_consecutive_index = 24
        :returns: The first index where a signal is detected for at least 5 consecutive periods.
        Note: If no such signal is detected, return None.
        """
        consecutive_count = 0
        first_consecutive_index = None

        for i in range(1, len(signal_indices)):
            if signal_indices[i] == signal_indices[i - 1] + 1:
                consecutive_count += 1
                # This means there are at least {CONSECUTIVE_SIGNAL_PERIODS} consecutive signals
                if consecutive_count >= self.CONSECUTIVE_SIGNAL_PERIODS - 1:
                    first_consecutive_index = signal_indices[i - self.CONSECUTIVE_SIGNAL_PERIODS + 1]
                    break
            else:
                consecutive_count = 0

        return first_consecutive_index

    def preprocess_data(self, df: pd.DataFrame, **kwargs) -> Any:
        """
        Perform common data preprocessing steps.

        Args:
            df (pd.DataFrame): The input DataFrame to preprocess.
            **kwargs: Additional keyword arguments for preprocessing.

        Returns:
            pd.DataFrame: The preprocessed DataFrame.
        """
        # Convert 'date' column to datetime
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        # Convert 'value' column to numeric and drop duplicate 'date' rows
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        # Drop duplicate 'date' rows
        df = df.drop_duplicates(subset=["date"]).reset_index(drop=True)
        # todo: should we drop 0 values?
        return df

    def validate_input(self, df: pd.DataFrame, **kwargs):
        """
        Validate the input DataFrame for the Process Control Analyzer.

        :param df: The input DataFrame to validate.
        """
        # check if the minimum of 10 data points is available for analysis
        if len(df) < 10:
            raise InsufficientDataError()

    def analyze(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Perform time series analysis using process control techniques.

        Args:
            df (pd.DataFrame): The input time series data with 'date' and 'value' columns.

        Returns:
            pd.DataFrame: The analyzed time series data with additional calculated columns.
        """
        start_index = 0
        central_line = []
        ucl = []
        lcl = []
        signal_indices = []
        slopes = []
        while start_index < len(df):
            # Compute end index based on available data points
            end_index = min(start_index + 18, len(df))

            # Compute half-averages for the current range
            half_averages = self.compute_half_averages(df, start_index, end_index)

            # Compute central line and slope based on half-averages
            central_line_range, slope = self.compute_central_line(
                half_averages, start_index, end_index, len(df) - start_index
            )
            central_line.extend(central_line_range)
            slopes.extend([slope] * len(central_line_range))

            # Compute upper and lower control limits based on central line
            ucl_range, lcl_range = self.compute_control_limits(df[start_index:end_index], central_line_range)
            ucl.extend(ucl_range)
            lcl.extend(lcl_range)

            # Detect signal in the current range using Wheeler rules
            signal_indices_range = self.detect_signal(df[start_index:], central_line_range, ucl_range, lcl_range)
            first_signal_index = self.is_recalculation_needed(signal_indices_range)
            if first_signal_index is not None:
                logger.info("Signal detected for at least 5 consecutive periods at index %s", first_signal_index)
                signal_indices.append(first_signal_index)
                # check if the minimum of 10 data points is available after the first signal
                if len(df) - first_signal_index < 10:
                    logger.info("Insufficient data points available for recalculation")
                    break
                prev_start_index = start_index
                start_index = first_signal_index
                # if stuck in a loop, increment the start index by 1
                if prev_start_index == start_index:
                    start_index += 1
                central_line = central_line[:start_index]
                slopes = slopes[:start_index]
                ucl = ucl[:start_index]
                lcl = lcl[:start_index]
            else:
                break

        # Add computed values to the DataFrame
        df["central_line"] = central_line + [None] * (len(df) - len(central_line))
        df["slope"] = slopes + [None] * (len(df) - len(slopes))
        # percentage change in a slope
        df["slope_change"] = df["slope"].pct_change() * 100
        # set slope change column to float
        df["slope_change"] = df["slope_change"].astype("float64")
        df["ucl"] = ucl + [None] * (len(df) - len(ucl))
        df["lcl"] = lcl + [None] * (len(df) - len(lcl))
        df["trend_signal_detected"] = df.index.isin(signal_indices)

        return df

    def post_process_result(self, result: Any) -> Any:
        """
        Perform common postprocessing steps on the analysis result.

        Args:
            result (pd.DataFrame): The analysis result to postprocess.

        Returns:
            Any: The postprocessed analysis results.
        """
        df = result
        # replace np.nan with None
        df.replace(np.nan, None, inplace=True)
        return df
