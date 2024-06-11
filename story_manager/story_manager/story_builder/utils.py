from datetime import datetime

import pandas as pd

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


def get_story_date(df: pd.DataFrame) -> datetime:
    story_date = df["date"].iloc[-1]
    return story_date


# story_date = ref_data["date"]
# df["date"].iloc[-1]
# df["date"].iloc[-1]
