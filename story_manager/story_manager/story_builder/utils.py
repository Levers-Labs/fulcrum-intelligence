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


def convert_snake_case_to_label(snake_case_str: str) -> str:
    words = snake_case_str.split("_")
    label_string = " ".join(word.capitalize() for word in words)
    return label_string


def fetch_dimensions_from_metric(metric_details: dict) -> list[str]:
    dimensions = [dimension["id"] for dimension in metric_details["dimensions"]]
    return dimensions
