import pandas as pd

from story_manager.core.enums import StoryType


def get_story_type_for_df(df: pd.DataFrame) -> pd.Series:
    """
    Determines the story type for each row in a DataFrame based on the 'value' and 'target' columns.

    For each row, the function checks:
    - If either 'value' or 'target' is NaN, it returns None.
    - If 'value' is greater than or equal to 'target', it returns StoryType.ON_TRACK.
    - Otherwise, it returns StoryType.OFF_TRACK.

    :param df: the dataframe.
    :return: string indicating the status of the story and None if the target is not available.
    """

    def determine_story_type(df_row: pd.Series) -> StoryType | None:
        value = df_row["value"]
        target = df_row["target"]
        if pd.isnull(target) or pd.isnull(value):
            return None
        elif value >= target:
            return StoryType.ON_TRACK
        else:
            return StoryType.OFF_TRACK

    return df.apply(determine_story_type, axis=1)
