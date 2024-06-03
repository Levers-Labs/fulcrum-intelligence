import logging
from datetime import date, timedelta

import pandas as pd
from dateutil.relativedelta import relativedelta

from commons.models.enums import Granularity
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.story_builder import StoryBuilderBase

logger = logging.getLogger(__name__)


class LikelyStatusStoryBuilder(StoryBuilderBase):
    genre = StoryGenre.PERFORMANCE
    group = StoryGroup.LIKELY_STATUS
    supported_grains = [Granularity.DAY, Granularity.WEEK, Granularity.MONTH]

    async def generate_stories(self, metric_id: str, grain: Granularity) -> list[dict]:
        """
        Generate likely status stories for the given metric and grain.
        Analyze the process control response DataFrame to identify likely status.

        Logic:
        - A time series of metric actuals and target values is constructed for the grain and
        span relevant to the digest.
        - The Forecasting module is used to forecast the metric through to the end of the relevant interval
        for the digest. For now, this can be a simple univariate forecast and need not be hierarchical.
        All that is needed of the forecast is the point estimate, and not the confidence bounds.
        - If the end-of-period forecast matches or exceeds the target value for the end of the period,
        a Likely On Track story is created. Else, the story is Likely Off Track.

        Exception handling:
            If no target is available, no story is created.

        :param metric_id: The metric ID for which likely status stories are generated.
        :param grain: The grain of the time series data.

        :return: A list containing a likely_status story dictionary.
        """
        stories: list[dict] = []

        # get metric details
        metric = await self.query_service.get_metric(metric_id)

        # find the start and end date for the input time series data
        start_date, end_date = self._get_input_time_range(grain)

        # get time series data with targets
        df = await self._get_time_series_data_with_targets(metric_id, grain, start_date, end_date)

        # validate time series data has minimum required data points
        time_durations = self.get_time_durations(grain)
        if len(df) < time_durations["min"]:
            logger.warning(
                "Discarding story generation for metric '%s' with grain '%s' due to insufficient data", metric_id, grain
            )
            return []

        # get the start and end of the story period
        interval, story_start_date, story_end_date = self._get_story_period(grain)

        # get the target value for the end of the period
        target_df = await self._get_time_series_for_targets(metric_id, grain, end_date, story_end_date)
        # Get the target value for the end of the period
        target_value = self.get_target_value_for_date(target_df, story_end_date)
        if pd.isnull(target_value) or target_value is None:
            logger.warning(
                "Discarding story generation for metric '%s' with grain '%s' due to missing target value",
                metric_id,
                grain,
            )
            return []

        # forecast the metric till the end of the period
        forecast_values = self.analysis_manager.simple_forecast(
            df=df.copy(deep=True), grain=grain, forecast_till_date=story_end_date  # type: ignore
        )

        # get the forecasted value for the end of the period
        forecasted_value = self.get_forecasted_value_for_date(forecast_values, story_end_date)
        if forecasted_value is None:
            logger.warning(
                "Discarding story generation for metric '%s' with grain '%s' due to missing forecasted value",
                metric_id,
                grain,
            )
            return []

        # prepare story df for rows greater than story_start_date
        story_df = self.prepare_forecasted_story_df(
            forecast_values, df=df, target_df=target_df, story_start_date=story_start_date
        )

        # create the likely status story based on the forecasted value and target
        if forecasted_value >= target_value:
            story_type = StoryType.LIKELY_ON_TRACK
        else:
            story_type = StoryType.LIKELY_OFF_TRACK

        # calculate deviation % of value from the target
        deviation = self.analysis_manager.calculate_percentage_difference(forecasted_value, target_value)
        # prepare story details
        story_details = self.prepare_story_dict(
            story_type,
            grain=grain,
            metric=metric,
            df=story_df,
            deviation=abs(deviation),
            forecasted_value=forecasted_value,
            target=target_value,
            interval=interval.value,
        )
        stories.append(story_details)
        logger.info(f"Stories generated for metric '{metric_id}', story details: {story_details}")
        return stories

    @staticmethod
    def prepare_forecasted_story_df(
        forecast_values: list[dict], df: pd.DataFrame, target_df: pd.DataFrame, story_start_date: date  # noqa
    ) -> pd.DataFrame:
        """
        Prepare a DataFrame with forecasted value, target, forecast till date, and interval.

        :param forecast_values: List of forecasted values.
        :param df: DataFrame containing the time series data.
        :param target_df: DataFrame containing target values.
        :param story_start_date: Start date of the story period.

        :return: DataFrame containing forecasted values, target, forecast till date, and interval.
        """
        forecast_df = pd.DataFrame(forecast_values, columns=["date", "value"])
        forecast_df["date"] = pd.to_datetime(forecast_df["date"])
        # Add a 'forecasted' flag to the forecast_df and df
        df["forecasted"] = False
        forecast_df["forecasted"] = True
        # Merge forecast_df with target_df to include target values for forecasted dates
        forecast_df = pd.merge(forecast_df, target_df[["date", "target"]], on="date", how="left")
        # Perform a union operation to combine the rows from df and forecast_df
        final_df = pd.concat([df, forecast_df], ignore_index=True)  # noqa

        # prepare out df for rows greater than story_start_date
        final_df = final_df.loc[final_df["date"] >= pd.to_datetime(story_start_date)]
        return final_df

    @staticmethod
    def get_target_value_for_date(target_df: pd.DataFrame, ref_date: date) -> float | None:
        """
        Get the target value for the given date.

        :param target_df: DataFrame containing target values.
        :param ref_date: Date for which the target value is required.

        :return: Target value for the given date.
        """
        target_value_series = target_df.loc[target_df["date"] == pd.to_datetime(ref_date), "target"]
        if target_value_series.empty:
            return None
        return target_value_series.item()

    @staticmethod
    def get_forecasted_value_for_date(forecast_values: list[dict], ref_date: date) -> float | None:
        """
        Get the forecasted value for the given date.

        :param forecast_values: List of forecasted values.
        :param ref_date: Date for which the forecasted value is required.

        :return: Forecasted value for the given date.
        """
        forecasted_value = next((fv["value"] for fv in forecast_values if fv["date"] == ref_date), None)
        return forecasted_value

    @staticmethod
    def _get_story_period(grain: Granularity, curr_date: date | None = None) -> tuple[Granularity, date, date]:
        """
        Get the interval, start date, and end date for the story period based
        on the grain and current date.
        """
        today = curr_date or date.today()
        if grain == Granularity.DAY:
            interval = Granularity.WEEK
            # end of the week
            end_date = today + timedelta(days=(6 - today.weekday()))
            # start of the week
            start_date = today - timedelta(days=today.weekday())
        elif grain == Granularity.WEEK:
            interval = Granularity.MONTH
            # start of last week of the month
            last_day_of_month = (today + relativedelta(months=1)).replace(day=1) - timedelta(days=1)
            end_date = last_day_of_month - timedelta(days=last_day_of_month.weekday())
            # start of the month
            start_date = last_day_of_month.replace(day=1)
        elif grain == Granularity.MONTH:
            interval = Granularity.QUARTER
            # start of quarter-end month
            quarter_end_month = (today.month - 1) // 3 * 3 + 3
            end_date = today.replace(month=quarter_end_month, day=1)
            # start of the quarter
            start_date = end_date - relativedelta(months=2)
        else:
            raise ValueError(f"Unsupported grain: {grain}")

        return interval, start_date, end_date
