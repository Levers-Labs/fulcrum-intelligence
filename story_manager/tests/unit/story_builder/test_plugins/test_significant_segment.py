from datetime import date

import pytest

from commons.models.enums import Granularity
from story_manager.story_builder.plugins.significant_segment import SignificantSegmentStoryBuilder

start_date = date(2024, 3, 1)
metric_id = "NewBizDeals"

grain = Granularity.WEEK


@pytest.fixture
def significant_segment_story_builder(
    mock_segment_drift_query_service, mock_analysis_service, mock_db_session, mock_analysis_manager
):
    builder = SignificantSegmentStoryBuilder(
        mock_segment_drift_query_service, mock_analysis_service, mock_analysis_manager, mock_db_session
    )
    builder.story_date = start_date
    return builder


@pytest.mark.asyncio
async def test_generate_stories_success(
    significant_segment_story_builder,
    metric_details,
    significant_segment_stories,
    dimension_slice_data,
    mock_get_dimension_slice_data,
):
    # The mock_get_dimension_slice_data fixture should now be properly set up
    stories = await significant_segment_story_builder.generate_stories(metric_id, grain)

    # Compare the generated stories with the expected stories
    assert len(stories) == len(significant_segment_stories)
    for generated, expected in zip(stories, significant_segment_stories):
        assert generated == expected
