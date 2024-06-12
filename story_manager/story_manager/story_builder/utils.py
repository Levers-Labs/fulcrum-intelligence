from datetime import date

import pandas as pd

from commons.models.enums import Granularity
from story_manager.core.enums import StoryType


def determine_status_for_value_and_target(df_row: pd.Series) -> StoryType | None:
    """
    Determines the status for each row in a DataFrame based on the 'value' and 'target' columns.

    For each row, the function checks:
    - If either 'value' or 'target' is NaN, it returns None.
    - If 'value' is greater than or equal to 'target', it returns StoryType.ON_TRACK.
    - Otherwise, it returns StoryType.OFF_TRACK.

    :param df_row: row of the dataframe.
    :return: string indicating the status of the story and None if the target is not available.
    """
    value = df_row["value"]
    target = df_row["target"]
    if pd.isnull(target) or pd.isnull(value):
        return None
    elif value >= target:
        return StoryType.ON_TRACK
    else:
        return StoryType.OFF_TRACK


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


def calculate_periods_count(from_date: date, to_date: date, grain: Granularity) -> int:
    """
    Calculate no. of periods between two given dates based on the specified granularity.
    :param from_date: the date from which we need to calculate the periods.
    :param to_date: the date to which we need to calculate the periods.
    :param grain: the type of granularity to calculate the periods.
    :return: no. of periods based on the specified granularity.
    """
    if grain == Granularity.DAY:
        count = abs((from_date - to_date).days)
    elif grain == Granularity.WEEK:
        weeks_difference = (from_date - to_date).days // 7
        # Adjust the weeks difference if the remainder of the days is not zero
        if (from_date - to_date).days % 7 != 0:
            weeks_difference += 1
        count = abs(weeks_difference)
    elif grain == Granularity.MONTH:
        # Calculate the difference in months
        months_difference = (from_date.year - to_date.year) * 12 + (from_date.month - to_date.month)
        count = abs(months_difference)
    else:
        raise ValueError(f"Unsupported grain: {grain}")
    return count


def fetch_dimensions_from_metric(metric_details: dict) -> list[str]:
    dimensions = [dimension["id"] for dimension in metric_details["dimensions"]]
    return dimensions


def convert_snake_case_to_label(snake_case_str: str) -> str:
    words = snake_case_str.split("_")
    label_string = " ".join(word.capitalize() for word in words)
    return label_string
