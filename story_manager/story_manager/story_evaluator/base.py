"""
Base classes for story evaluators.

Story evaluators analyze pattern outputs and generate stories based on specific conditions.
"""

import logging
from abc import ABC, abstractmethod
from typing import (
    Any,
    Generic,
    TypeVar,
    cast,
)

import numpy as np
import pandas as pd

from commons.models.enums import Granularity
from commons.utilities.grain_utils import GRAIN_META
from levers.models.common import BasePattern
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.story_evaluator.constants import (
    STORY_GROUP_TIME_DURATIONS,
    STORY_TEMPLATES,
    STORY_TYPE_TIME_DURATIONS,
)
from story_manager.story_evaluator.utils import format_date_column

T = TypeVar("T", bound=BasePattern)
logger = logging.getLogger(__name__)


class StoryEvaluatorBase(Generic[T], ABC):
    """
    Base class for story evaluators.

    Story evaluators take pattern results as input and generate stories based on
    evaluation of the pattern data.
    """

    pattern_name: str
    # decimal precision
    precision = 3

    def __init__(self, series_df: pd.DataFrame | None = None):
        self.series_df = series_df

    @abstractmethod
    async def evaluate(self, pattern_result: T, metric: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Evaluate the pattern result and generate stories.

        Args:
            pattern_result: The pattern result to evaluate
            metric: Details about the metric

        Returns:
            list of story dictionaries
        """
        pass

    def fill_missing_dates(self, df: pd.DataFrame, grain: Granularity, fill_value: float = 0.0) -> pd.DataFrame:
        """
        Fill missing dates in the series.

        Args:
            df: DataFrame with time series data containing 'date' and 'value' columns
            grain: Granularity to determine the appropriate date frequency
            fill_value: Value to use for filling missing dates and null values (default: 0.0)

        Returns:
            DataFrame with missing dates filled
        """
        if df is None or df.empty or "date" not in df.columns:
            return df

        # Make a copy to avoid modifying the original
        df_filled = df.copy()

        # Ensure date column is datetime
        df_filled["date"] = pd.to_datetime(df_filled["date"])

        # Replace inf, -inf, and NaN with fill_value in all numeric columns
        numeric_columns = df_filled.select_dtypes(include=[np.number]).columns
        df_filled[numeric_columns] = df_filled[numeric_columns].replace(
            [float("inf"), float("-inf"), np.NaN], fill_value
        )

        # If we have at least 2 dates, fill missing dates in the range
        if len(df_filled) >= 2:
            # Sort by date
            df_filled = df_filled.sort_values("date")

            # Determine frequency based on grain
            freq_map = {
                Granularity.DAY: "D",  # Daily
                Granularity.WEEK: "W-MON",  # Weekly, starting on Monday
                Granularity.MONTH: "MS",  # Monthly, 1st of each month
                Granularity.QUARTER: "QS",  # Quarterly, 1st of each quarter
                Granularity.YEAR: "YS",  # Yearly, 1st of each year
            }

            freq = freq_map.get(grain, "D")  # Default to daily if grain not found

            # Create date range from min to max date
            date_range = pd.date_range(start=df_filled["date"].min(), end=df_filled["date"].max(), freq=freq)

            # Create a complete date index
            complete_df = pd.DataFrame({"date": date_range})

            # Merge with original data, filling missing values
            df_filled = complete_df.merge(df_filled, on="date", how="left")

            # Fill missing values with fill_value for all numeric columns
            numeric_columns = df_filled.select_dtypes(include=[np.number]).columns
            df_filled[numeric_columns] = df_filled[numeric_columns].fillna(fill_value)

            # For categorical columns like dimension_slice, forward fill
            categorical_columns = ["dimension_slice", "segment", "label"]
            for col in categorical_columns:
                if col in df_filled.columns:
                    df_filled[col] = df_filled[col].ffill().bfill()

        return df_filled

    def prepare_story_model(
        self,
        genre: StoryGenre,
        story_type: StoryType,
        story_group: StoryGroup,
        metric_id: str,
        pattern_result: T,
        title: str,
        detail: str,
        grain: Granularity,
        series_data: dict[str, Any] | list[dict[str, Any]] | None = None,
        **extra_data,
    ) -> dict[str, Any]:
        """
        Prepare a story model dictionary.

        Args:
            genre: The genre of story
            story_type: The type of story
            story_group: The group of the story
            metric_id: The ID of the metric
            pattern_result: The pattern result
            metric_id: Details about the metric
            title: The story title
            detail: The story detail text
            grain: Granularity of the analysis
            series_data: Series data to include in the story
            extra_data: Additional data to include in the story

        Returns:
            Story model dictionary
        """
        # Get the story date from the pattern result
        story_date = pattern_result.analysis_date

        if series_data is None:
            series_data = self.export_dataframe_as_story_series(
                self.series_df,
                story_type,
                story_group,
                grain,  # type: ignore
            )

        return {
            "version": 2,
            "genre": genre,
            "story_type": story_type,
            "story_group": story_group,
            "grain": grain,
            "metric_id": metric_id,
            "title": title,
            "detail": detail,
            "title_template": self.get_template_string(story_type, "title"),
            "detail_template": self.get_template_string(story_type, "detail"),
            "story_date": story_date,
            "variables": extra_data,
            "series": series_data,
            "metadata": dict(pattern=pattern_result.pattern),
            "pattern_run_id": pattern_result.pattern_run_id,
        }

    def get_template_string(self, story_type: StoryType, field: str) -> str:
        """
        Get the template string for a story type and field.

        Args:
            story_type: The type of story
            field: The field to get the template for (title or detail)

        Returns:
            The template string
        """
        story_templates = STORY_TEMPLATES.get(story_type, {})
        return story_templates.get(field, "")

    def export_dataframe_as_story_series(
        self, series_df: pd.DataFrame | None, story_type: StoryType, story_group: StoryGroup, grain: Granularity
    ) -> list[dict[str, Any]]:
        """
        Format the time series data for story display.

        Args:
            series_df: Time series data
            story_type: Type of the story
            story_group: Group of the story
            grain: Granularity for which the story is generated

        Returns:
            dictionary with formatted time series data and analytics
        """
        if series_df is None or series_df.empty:
            return []

        # Fill missing dates
        series_df = self.fill_missing_dates(series_df, grain)

        # Figure out the length of the series to export
        series_length = self.get_output_length(story_type, story_group, grain)

        # Convert the date column to datetime and then to ISO format strings
        if "date" in series_df.columns:
            series_df = format_date_column(series_df)

        # Get the last n rows
        series = series_df.tail(series_length) if series_length else series_df

        # Final cleanup: Replace any remaining NaN/inf values before converting to dict
        series = series.replace([float("inf"), float("-inf"), np.NaN], 0.0)

        # Add the time series data to the result
        data = series.to_dict(orient="records")
        return cast(list[dict[str, Any]], data)

    def prepare_base_context(self, metric: dict, grain: Granularity) -> dict[str, Any]:
        """
        Prepare the base context for the story.
        """
        grain_info = GRAIN_META.get(grain, {"label": "period", "pop": "PoP"})  # type: ignore
        metric_info = {
            "label": metric["label"],
            "metric_id": metric["metric_id"],
        }
        return {"metric": metric_info, "grain_label": grain_info["label"], "pop": grain_info["pop"]}

    def get_output_length(self, story_type: StoryType, story_group: StoryGroup, grain: Granularity) -> int | None:
        """
        Get the output length for the given grain.

        Args:
            story_type: The story type for which the time durations are retrieved
            grain: The grain for which the time durations are retrieved

        Returns:
            Integer value of the time duration or None if the story type and grain are not in the supported list
        """
        # check first we have by story type if not then by story group
        if story_type in STORY_TYPE_TIME_DURATIONS:
            grain_durations = STORY_TYPE_TIME_DURATIONS[story_type][grain]
        elif story_group in STORY_GROUP_TIME_DURATIONS:
            grain_durations = STORY_GROUP_TIME_DURATIONS[story_group][grain]
        else:
            return None

        return grain_durations["output"]

    async def run(self, pattern_result: T, metric: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Run the story evaluator on the pattern result.

        Args:
            pattern_result: The pattern result to evaluate
            metric: Details about the metric

        Returns:
            list of generated story dictionaries
        """
        logger.info(f"Running story evaluator for pattern {self.pattern_name}")

        # Generate stories from pattern result
        stories = await self.evaluate(pattern_result, metric)

        if not stories:
            logger.info("No stories generated for pattern %s", self.pattern_name)
            return []

        logger.info(f"Generated {len(stories)} stories for pattern {self.pattern_name}")
        return stories
