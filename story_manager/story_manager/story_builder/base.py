import logging
from abc import ABC, abstractmethod
from datetime import date, timedelta
from typing import Any

import pandas as pd
from jinja2 import Template

from commons.models.enums import Granularity
from story_manager.core.enums import STORY_TYPES_META, StoryGenre, StoryType

logger = logging.getLogger(__name__)


class StoryBuilderBase(ABC):
    """
    Abstract base class for story builders
    """

    genre: StoryGenre
    supported_grains: list[Granularity] = []
    grain_meta: dict[str, Any] = {
        Granularity.DAY: {"comp_label": "d/d", "delta": {"days": 1}},
        Granularity.WEEK: {"comp_label": "w/w", "delta": {"weeks": 1}},
        Granularity.MONTH: {"comp_label": "m/m", "delta": {"months": 1}},
        Granularity.QUARTER: {"comp_label": "q/q", "delta": {"months": 3}},
        Granularity.YEAR: {"comp_label": "y/y", "delta": {"years": 1}},
    }

    def __init__(self, query_service, analysis_service, db_session):
        """
        Initialize the StoryBuilderBase instance
        :param query_service: QueryService instance for retrieving data
        :param analysis_service: AnalysisService instance for performing analysis
        :param db_session: Database session for persisting stories
        """
        self.query_service = query_service
        self.analysis_service = analysis_service
        self.db_session = db_session

    def _get_time_series_data(
        self, metric_id: str, grain: Granularity, start_date: date, end_date: date
    ) -> pd.DataFrame:
        """
        Retrieve time series data for the given metric, grain, and date range

        :param metric_id: The metric ID for which time series data is retrieved
        :param grain: The grain for which time series data is retrieved
        :param start_date: The start date of the time series data
        :param end_date: The end date of the time series data
        :return: A pandas DataFrame containing the time series data
        """
        logger.debug(
            f"Retrieving time series data for metric '{metric_id}' with grain '{grain}' from {start_date} to {end_date}"
        )
        metric_values = self.query_service.get_metric_values(metric_id, grain, start_date, end_date)
        time_series_df = pd.DataFrame(metric_values, columns=["date", "value"])
        time_series_df["date"] = pd.to_datetime(time_series_df["date"])
        time_series_df.set_index("date", inplace=True)
        return time_series_df

    def _render_story_text(self, story_type: StoryType, **context) -> str:
        """
        Render the story text using the story type and context variables

        :param story_type: The type of the story
        :param context: Additional context variables required for rendering the story text
        :return: The rendered story text
        """
        logger.debug(f"Rendering story text for story type '{story_type}'")
        story_meta = STORY_TYPES_META[story_type]
        template = Template(story_meta["template"])
        return template.render(**context)

    @abstractmethod
    async def generate_stories(self, metric_id: str, grain: Granularity) -> list[dict]:
        """
        Generate stories for the given metric and grain
        :param metric_id: The metric ID for which stories are generated
        :param grain: The grain for which stories are generated
        :return: A list of generated stories
        """
        pass

    def run(self, metric_id: str, grain: Granularity) -> None:
        """
        Run the story generation process for the given metric and grain

        :param metric_id: The metric ID for which stories are generated.
        :param grain: The grain for which stories are generated
        """
        if grain not in self.supported_grains:
            logger.warning(f"Unsupported grain '{grain}' for story genre '{self.genre}'")
            raise ValueError(f"Unsupported grain '{grain}' for story genre '{self.genre}'")

        logger.info(f"Generating stories for metric '{metric_id}' with grain '{grain}'")
        stories = self.generate_stories(metric_id, grain)
        self.persist_stories(stories)  # type: ignore

    def persist_stories(self, stories: list[dict]) -> None:
        """
        Persist the generated stories in the database

        :param stories: The list of generated stories
        """
        # perform the necessary data transformations
        # persist the stories in the database
        logger.info(f"Persisting {len(stories)} stories in the database")
        self.db_session.add_all(stories)
        self.db_session.commit()
        logger.info("Stories persisted successfully")

    @classmethod
    def _get_current_period_range(cls, grain: str, curr_date: date | None = None) -> tuple[date, date]:
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
