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
