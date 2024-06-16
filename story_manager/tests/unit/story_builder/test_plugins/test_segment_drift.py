from datetime import date
from unittest.mock import AsyncMock

import pytest

from commons.models.enums import Granularity
from fulcrum_core.modules.segment_drift import SegmentDriftEvaluator
from story_manager.story_builder.plugins.segment_drift import SegmentDriftStoryBuilder

start_date = date(2024, 3, 1)
# number_of_data_points = 90
metric_id = "NewBizDeals"

grain = Granularity.WEEK


@pytest.fixture
def segment_drift_story_builder(mock_query_service, mock_analysis_service, mock_analysis_manager, mock_db_session):
    return SegmentDriftStoryBuilder(mock_query_service, mock_analysis_service, mock_analysis_manager, mock_db_session)


@pytest.mark.asyncio
async def test_generate_stories_success(
    segment_drift_story_builder, segment_drift_stories, segment_drift_data, metric_details
):
    segment_drift_story_builder.query_service.get_metric = AsyncMock(return_value=metric_details)
    SegmentDriftEvaluator.calculate_segment_drift = AsyncMock(return_value=segment_drift_data)
    stories = await segment_drift_story_builder.generate_stories(metric_id, grain)
    assert stories == segment_drift_stories


@pytest.mark.asyncio
async def test_generate_stories_shrinking_segment(
    segment_drift_story_builder,
    shrinking_and_improving_stories,
    segment_drift_data,
    metric_details,
):
    segment_drift_story_builder.query_service.get_metric = AsyncMock(return_value=metric_details)

    # removing growing segment from dimension slices
    segment_drift_data["dimension_slices"] = segment_drift_data["dimension_slices"][:1]

    SegmentDriftEvaluator.calculate_segment_drift = AsyncMock(return_value=segment_drift_data)
    stories = await segment_drift_story_builder.generate_stories(metric_id, grain)
    assert stories == shrinking_and_improving_stories


@pytest.mark.asyncio
async def test_generate_stories_improving_segment(
    segment_drift_story_builder, shrinking_and_improving_stories, segment_drift_data, metric_details
):
    segment_drift_story_builder.query_service.get_metric = AsyncMock(return_value=metric_details)

    # removing growing segment from dimension slices
    segment_drift_data["dimension_slices"] = segment_drift_data["dimension_slices"][:1]

    SegmentDriftEvaluator.calculate_segment_drift = AsyncMock(return_value=segment_drift_data)
    stories = await segment_drift_story_builder.generate_stories(metric_id, grain)
    assert stories == shrinking_and_improving_stories
