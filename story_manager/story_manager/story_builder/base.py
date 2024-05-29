import logging
from abc import ABC, abstractmethod
from collections.abc import Hashable
from datetime import date, timedelta
from typing import Any

import numpy as np
import pandas as pd
from jinja2 import Template
from sqlmodel.ext.asyncio.session import AsyncSession

from commons.clients.analysis_manager import AnalysisManagerClient
from commons.clients.query_manager import QueryManagerClient
from commons.models.enums import Granularity
from fulcrum_core import AnalysisManager
from story_manager.core.enums import (
    STORY_TYPES_META,
    StoryGenre,
    StoryGroup,
    StoryType,
)
from story_manager.core.models import Story
from story_manager.story_builder.constants import GRAIN_META, STORY_GROUP_TIME_DURATIONS

logger = logging.getLogger(__name__)


class StoryBuilderBase(ABC):
    """
    Abstract base class for story builders
    """

    genre: StoryGenre
    group: StoryGroup
    supported_grains: list[Granularity] = []
    # decimal precision
    precision = 3
    # date text format
    date_text_format = "%B %d, %Y"

    def __init__(
        self,
        query_service: QueryManagerClient,
        analysis_service: AnalysisManagerClient,
        analysis_manager: AnalysisManager,
        db_session: AsyncSession,
    ):
        """
        Initialize the StoryBuilderBase instance
        :param query_service: QueryService instance for retrieving data
        :param analysis_service: AnalysisService instance for performing analysis via analysis manager api
        :param analysis_manager: AnalysisManager instance for performing analysis
        that directly interacts with the analysis manager core library
        :param db_session: Database session for persisting stories
        """
        self.query_service = query_service
        self.analysis_service = analysis_service
        self.analysis_manager = analysis_manager
        self.db_session = db_session

    async def _get_time_series_data(
        self, metric_id: str, grain: Granularity, start_date: date, end_date: date, set_index: bool = False
    ) -> pd.DataFrame:
        """
        Retrieve time series data for the given metric, grain, and date range

        :param metric_id: The metric ID for which time series data is retrieved
        :param grain: The grain for which time series data is retrieved
        :param start_date: The start date of the time series data
        :param end_date: The end date of the time series data
        :param set_index: Whether to set the date column as the index of the DataFrame
        :return: A pandas DataFrame containing the time series data
        """
        logger.debug(
            f"Retrieving time series data for metric '{metric_id}' with grain '{grain}' from {start_date} to {end_date}"
        )
        metric_values = await self.query_service.get_metric_time_series(
            metric_id, start_date=start_date, end_date=end_date, grain=grain
        )
        time_series_df = pd.DataFrame(metric_values, columns=["date", "value"])
        time_series_df["date"] = pd.to_datetime(time_series_df["date"])
        if set_index:
            time_series_df.set_index("date", inplace=True)
        return time_series_df

    def get_story_context(self, grain: Granularity, metric: dict, **context) -> dict[str, Any]:
        """
        Get the context variables required for rendering the story detail and title

        The base class will add some of the default context variables like
        * eoi* → end of interval (EOD, EOW, EOM)
        * interval* → daily, weekly, monthly
        * grain* → day, week, month
        * pop* → period over period (d/d , w/w, m/m)

        Provided context variables will be added to the context dictionary

        :param grain: The grain for which the context variables are retrieved
        :param metric: The metric for which the context variables are retrieved
        :param context: Additional context variables
        :return: A dictionary containing the context variables
        """
        logger.debug(f"Getting story context for grain '{grain}' and metric '{metric['id']}'")
        grain_meta = GRAIN_META[grain]
        _context = {
            "grain": grain.value,
            "eoi": grain_meta["eoi"],
            "metric": {"id": metric["id"], "label": metric["label"]},
            "pop": grain_meta["pop"],
            "interval": grain_meta["interval"],
        }
        # update the context with additional context variables
        _context.update(context)
        return _context

    def _render_story_texts(
        self, story_type: StoryType, grain: Granularity, metric: dict, **extra_context
    ) -> dict[str, Any]:
        """
        Render the story title and detail using the story type and context variables

        :param story_type: The type of the story
        :param context: Additional context variables required for rendering the story title and detail
        :return: The dict containing the rendered story title and detail as well as the context variables
        """
        logger.debug(f"Rendering story texts for story type '{story_type}'")

        context = self.get_story_context(grain, metric, **extra_context)

        # get the story meta-data
        story_meta = STORY_TYPES_META[story_type]
        # render the story title and detail
        title = Template(story_meta["title"])
        detail = Template(story_meta["detail"])
        return {
            "title": title.render(context),
            "detail": detail.render(context),
            "title_template": story_meta["title"],
            "detail_template": story_meta["detail"],
            "variables": context,
        }

    def prepare_story_dict(
        self, story_type: StoryType, grain: Granularity, metric: dict, df: pd.DataFrame, **extra_context
    ) -> dict[str, Any]:
        """
        Prepare the story dictionary with the required fields

        :param story_type: The type of the story
        :param grain: The grain for which the story is generated.
        :param metric: The metric for which the story is generated.
        :param df: The time series data for the story
        :param extra_context: Additional context variables for the story

        :return: A dictionary containing the story details
        """
        logger.debug(f"Preparing story dictionary for story type '{story_type}'")

        series = self.get_story_series(df, grain)
        story_texts = self._render_story_texts(story_type, grain=grain, metric=metric, **extra_context)

        return {
            "metric_id": metric["id"],
            "genre": self.genre,
            "story_group": self.group,
            "story_type": story_type,
            "grain": grain,
            "series": series,
            "title": story_texts["title"],
            "detail": story_texts["detail"],
            "title_template": story_texts["title_template"],
            "detail_template": story_texts["detail_template"],
            "variables": story_texts["variables"],
        }

    def get_story_series(self, df: pd.DataFrame, grain: Granularity) -> list[dict[Hashable, Any]]:
        """
        Format the time series data in the DataFrame by converting the 'date' column to a string format specified
        by 'date_text_format' attribute and replacing NaN values with the string 'None'.

        :param grain: Grain for which the story is generated.
        :param df: The DataFrame containing the time series data.
        :return: series: The DataFrame with formatted time series data.
        """
        grain_durations = self.get_time_durations(grain)
        series_length = grain_durations["output"]

        if "date" in df.columns:
            df["date"] = df["date"].dt.strftime(self.date_text_format) if hasattr(df["date"], "dt") else df["date"]
        df.replace([float("inf"), float("-inf"), np.NaN], [None, None, None], inplace=True)
        series = df.tail(series_length).to_dict(orient="records")
        return series

    @abstractmethod
    async def generate_stories(self, metric_id: str, grain: Granularity) -> list[dict]:
        """
        Generate stories for the given metric and grain
        :param metric_id: The metric ID for which stories are generated
        :param grain: The grain for which stories are generated
        :return: A list of generated stories
        """
        pass

    async def run(self, metric_id: str, grain: Granularity) -> None:
        """
        Run the story generation process for the given metric and grain

        :param metric_id: The metric ID for which stories are generated.
        :param grain: The grain for which stories are generated
        """
        if grain not in self.supported_grains:
            logger.warning(
                f"Unsupported grain '{grain}' for story genre '{self.genre.value}' of story group '{self.group.value}'"
            )
            raise ValueError(
                f"Unsupported grain '{grain.value}' for story genre '{self.genre.value}' of "
                f"story group '{self.group.value}'"
            )

        logger.info("Generating %s stories for metric '%s' with grain '%s'", self.group.value, metric_id, grain)
        stories = await self.generate_stories(metric_id, grain)
        logger.info("Generated %s stories for metric '%s' with grain '%s'", len(stories), metric_id, grain)
        await self.persist_stories(stories)

    async def persist_stories(self, stories: list[dict]):
        """
        Persist the generated stories in the database

        :param stories: The list of generated stories
        """
        logger.info(f"Persisting {len(stories)} stories in the database")
        logger.info(f"stories: {stories}")

        # perform the necessary data transformations
        story_objs: list[Story] = [Story.parse_obj(story_dict) for story_dict in stories]
        self.db_session.add_all(story_objs)
        await self.db_session.commit()
        logger.info("Stories persisted successfully")

    @classmethod
    def _get_current_period_range(cls, grain: Granularity, curr_date: date | None = None) -> tuple[date, date]:
        """
        Get the end date of the last period based on the grain.
        Based on the current date, the end date is calculated as follows:
        - For day grain: yesterday
        - For week grain: the last Sunday
        - For month grain: the last day of the previous month
        - For quarter grain: the last day of the previous quarter
        - For year grain: December 31 of the previous year
        For each grain, the start date of the period is calculated based on the end date.

        :param curr_date: The current date for which the period range is calculated.
        :param grain: The grain for which the end date is retrieved.
        :return: The start and end date of the period.
        """
        today = curr_date or date.today()
        if grain == Granularity.DAY:
            end_date = today - timedelta(days=1)
            start_date = end_date
        elif grain == Granularity.WEEK:
            end_date = today - timedelta(days=today.weekday() + 1)
            start_date = end_date - timedelta(days=6)
        elif grain == Granularity.MONTH:
            end_date = date(today.year, today.month, 1) - timedelta(days=1)
            start_date = date(end_date.year, end_date.month, 1)
        elif grain == Granularity.QUARTER:
            quarter_end_month = (today.month - 1) // 3 * 3
            end_date = date(today.year, quarter_end_month + 1, 1) - timedelta(days=1)
            start_date = date(end_date.year, end_date.month - 2, 1)
        elif grain == Granularity.YEAR:
            end_date = date(today.year - 1, 12, 31)
            start_date = date(end_date.year, 1, 1)
        else:
            raise ValueError(f"Unsupported grain: {grain}")
        return start_date, end_date

    def get_time_durations(self, grain: Granularity) -> dict[str, Any]:
        """
        Get the time durations for the given grain and group

        :param grain: The grain for which the time durations are retrieved
        :return: A dictionary containing the time durations
        """
        if self.group not in STORY_GROUP_TIME_DURATIONS or grain not in STORY_GROUP_TIME_DURATIONS[self.group]:
            raise ValueError(f"Unsupported group '{self.group}' or grain '{grain}'")
        return STORY_GROUP_TIME_DURATIONS[self.group][grain]

    def _get_input_time_range(self, grain: Granularity, curr_date: date | None = None) -> tuple[date, date]:
        """
        Get the time range for the input data based on the grain.

        :param curr_date: The current date for which the time range is calculated.
        :param grain: The grain for which the time range is retrieved.

        :return: The start and end date of the time range.
        """

        latest_start_date, latest_end_date = self._get_current_period_range(grain, curr_date)

        grain_durations = self.get_time_durations(grain)

        # figure out the number of grain deltas to go back
        period_count = grain_durations["input"]

        # figure out relevant grain delta .e.g weeks : 1
        delta_eq = GRAIN_META[grain]["delta"]

        # Go back by the determined grain delta
        delta = pd.DateOffset(**delta_eq)
        start_date = (latest_start_date - period_count * delta).date()

        return start_date, latest_end_date

    async def _get_time_series_data_with_targets(
        self, metric_id: str, grain: Granularity, start_date: date, end_date: date
    ) -> pd.DataFrame:
        """
        Retrieve time series data for the given metric, grain, and date range with target values.

        :param metric_id: The metric ID for which time series data is retrieved
        :param grain: The grain for which time series data is retrieved
        :param start_date: The start date of the time series data
        :param end_date: The end date of the time series data
        :return: A pandas DataFrame containing the time series data with target values.
        """

        logger.debug(
            f"Retrieving time series data with targets for metric '{metric_id}' with grain '{grain}' "
            f"from {start_date} to {end_date}"
        )

        series_df = await self._get_time_series_data(metric_id, grain, start_date, end_date, set_index=False)

        targets_list = await self.query_service.get_metric_targets(metric_id, grain, start_date, end_date)
        targets_df = pd.DataFrame(targets_list)

        # Converting date columns to datetime
        series_df["date"] = pd.to_datetime(series_df["date"])
        targets_df["target_date"] = pd.to_datetime(targets_df["target_date"])

        # Merging df with target_df on the date columns
        merged_df = pd.merge(series_df, targets_df, left_on="date", right_on="target_date", how="left")

        # Selecting only the required columns
        final_df = merged_df[["date", "value", "target_value"]]

        # Renaming the target_value column to target
        final_df = final_df.rename(columns={"target_value": "target"})
        return final_df
