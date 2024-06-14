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
    return SignificantSegmentStoryBuilder(
        mock_segment_drift_query_service, mock_analysis_service, mock_analysis_manager, mock_db_session
    )


@pytest.mark.asyncio
async def test_generate_stories_success(
    mocker,
    significant_segment_story_builder,
    metric_details,
    significant_segment_stories,
    dimension_slice_data,
    mock_get_dimension_slice_data,
):

    stories = await significant_segment_story_builder.generate_stories(metric_id, grain)
    assert stories == significant_segment_stories
