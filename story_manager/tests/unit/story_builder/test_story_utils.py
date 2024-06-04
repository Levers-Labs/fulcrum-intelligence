from datetime import datetime

import numpy as np
import pandas as pd

from story_manager.core.enums import StoryType
from story_manager.story_builder.utils import determine_status_for_value_and_target, get_target_value_for_date


def test_get_target_value_for_date():
    # Prepare
    target_df = pd.DataFrame(
        {
            "date": pd.date_range(start="2022-01-10", periods=5, freq="D"),
            "target": [100, 200, 300, 400, 500],
        }
    )
    ref_date = datetime(2022, 1, 12).date()

    # Act
    target_value = get_target_value_for_date(target_df, ref_date)

    # Assert
    assert target_value == 300

    # Prepare
    ref_date = datetime(2022, 1, 9).date()

    # Act
    target_value = get_target_value_for_date(target_df, ref_date)

    # Assert
    assert target_value is None


def test_determine_status_for_value_and_target():
    df = pd.DataFrame(
        {
            "value": [np.NaN, 200, 300, 200, np.NaN],
            "target": [100, np.NaN, 300, 400, np.NaN],
            "expected": [None, None, StoryType.ON_TRACK, StoryType.OFF_TRACK, None],
        }
    )

    for _, row in df.iterrows():
        res = determine_status_for_value_and_target(row)
        assert res == row["expected"]
