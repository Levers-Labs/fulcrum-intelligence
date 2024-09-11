import logging
from abc import ABC, abstractmethod
from typing import Any

import pandas as pd

from fulcrum_core.execptions import InsufficientDataError

logger = logging.getLogger(__name__)


class BaseAnalyzer(ABC):
    def __init__(self, precision: int = 3):
        """
        Initialize the BaseAnalyzer.

        Args:
            precision (int): The decimal precision for rounding values.
            Default is 3.
        """
        self.precision = precision

    @abstractmethod
    def analyze(self, df: pd.DataFrame, *args, **kwargs) -> dict[str, Any] | pd.DataFrame:
        """
        Abstract method to be implemented by derived analyzer classes.

        Args:
            df (pd.DataFrame): The input DataFrame for analysis.
            *args: Additional positional arguments for the analysis.
            **kwargs: Additional keyword arguments for the analysis.

        Returns:
            dict | pd.DataFrame: The analysis result as a dictionary or DataFrame.
        """
        pass

    @staticmethod
    def calculate_percent_change(comparison_value: float, base_value: float) -> float:
        """
        Calculate the percentage change between two values
        :param comparison_value: Value to compare
        :param base_value: Base value
        :return: Percentage change
        """
        if base_value == 0:
            return float("inf")

        return ((comparison_value - base_value) / base_value) * 100

    def validate_input(self, df: pd.DataFrame, **kwargs):
        """
        Validate the input DataFrame.

        Args:
            df (pd.DataFrame): The input DataFrame to validate.

        Raises:
            ValueError: If the input DataFrame is missing required columns.
        """
        logger.debug("Validating input data...")
        if df.empty:
            raise InsufficientDataError()

    def preprocess_data(self, df: pd.DataFrame, **kwargs) -> Any:
        """
        Perform common data preprocessing steps.

        Args:
            df (pd.DataFrame): The input DataFrame to preprocess.
            **kwargs: Additional keyword arguments for preprocessing.
            Override if input is not dataframe, or if preprocessing is not required.

        Returns:
            pd.DataFrame: The preprocessed DataFrame.
        """
        return df

    def post_process_result(self, result: Any) -> Any:
        """
        Perform common postprocessing steps on the analysis result.

        Args:
            result (list| dict | pd.DataFrame): The analysis result to postprocess.

        Returns:
            list| dict | pd.DataFrame: The postprocessed analysis result.
        """
        if isinstance(result, dict) or isinstance(result, pd.DataFrame):
            result["analyzed_at"] = pd.Timestamp.now()
        logger.debug("Result postprocessing completed.")
        return result

    def round_values(self, result: Any) -> Any:
        """
        round the numeric values in the result dictionary.

        Args:
            result (list| dict | pd.DataFrame): The result dictionary or DataFrame to round.

        Returns:
            list | dict | pd.DataFrame: The result dictionary or DataFrame with rounded values.
        """
        if isinstance(result, dict):
            for key, value in result.items():
                if isinstance(value, (float, int)):
                    result[key] = round(value, self.precision)
                elif isinstance(value, dict):
                    result[key] = self.round_values(value)
                elif isinstance(value, list):
                    result[key] = self.round_values(value)
            return result
        elif isinstance(result, list):
            new_list = []
            for item in result:
                if isinstance(item, dict):
                    new_list.append(self.round_values(item))
                elif isinstance(item, (float, int)):
                    new_list.append(round(item, self.precision))
                else:
                    new_list.append(item)
            return new_list
        else:
            df = result.round(self.precision)
            return df

    def run(self, df: pd.DataFrame, **kwargs) -> Any:
        """
        Run the complete analysis process.

        Args:
            df (pd.DataFrame): The input DataFrame for analysis.
            **kwargs: Additional keyword arguments for the analysis.

        Returns:
            dict | pd.DataFrame: The analysis result as a dictionary or DataFrame.
        """
        logger.info("Starting analysis...")
        self.validate_input(df, **kwargs)
        # Preprocess data
        preprocessed_df = self.preprocess_data(df)
        # Run analysis
        result = self.analyze(preprocessed_df, **kwargs)
        # Round values
        rounded_result = self.round_values(result)
        # Postprocess result
        result = self.post_process_result(rounded_result)

        logger.info("Analysis completed.")
        return result

    def merge_dataframes(self, dfs: list[pd.DataFrame]) -> pd.DataFrame:
        """
        Merges multiple data files or dataframes based on the 'date' column and returns the merged dataframe.
        The dataframes are expected to have a 'date' column and a 'value' column.
        The 'value' column is renamed to the 'metric_id' column value.
        """
        # create copies of dataframes
        dfs = [df.copy(deep=True) for df in dfs]
        # Fetching common dates in all features
        common_dates = dfs[0]["date"]
        for _df in dfs[1:]:
            common_dates = common_dates[common_dates.isin(_df["date"])]
        # Renaming the value column to metric_id and dropping the metric_id column
        # Keeping only those dates that are common
        for _df in dfs:
            # convert date column to datetime
            _df["date"] = pd.to_datetime(_df["date"])
            # convert value column to numeric
            _df["value"] = pd.to_numeric(_df["value"], errors="coerce")
            metric_id = _df["metric_id"].iloc[0]
            _df.rename(columns={"value": metric_id}, inplace=True)
            # drop metric_id column
            _df.drop(columns=["metric_id"], inplace=True)
            # Keeping only those dates that are common
            _df = _df[_df["date"].isin(common_dates)]

        # Merging all dataframes
        merged_df = dfs[0]
        for _df in dfs[1:]:
            merged_df = merged_df.merge(_df, on="date")

        # drop na
        merged_df = merged_df.dropna()
        # # drop date column
        # merged_df = merged_df.drop("date", axis=1)
        return merged_df
