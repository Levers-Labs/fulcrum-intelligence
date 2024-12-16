import pytest

from story_manager.core.enums import StoryGroup
from story_manager.story_builder import StoryBuilderBase
from story_manager.story_builder.factory import StoryFactory
from story_manager.story_builder.plugins.growth_rates import GrowthStoryBuilder


def test_story_factory_create_story_builder_growth(
    mock_query_service, mock_analysis_service, mock_analysis_manager, mock_db_session, mock_llm_provider
):
    story_builder = StoryFactory.create_story_builder(
        StoryGroup.GROWTH_RATES.value,
        mock_query_service,
        mock_analysis_service,
        mock_analysis_manager,
        mock_db_session,
        mock_llm_provider,
    )
    assert isinstance(story_builder, GrowthStoryBuilder)


def test_story_factory_create_story_builder_unsupported_group(
    mock_query_service, mock_analysis_service, mock_analysis_manager, mock_db_session
):
    with pytest.raises(ValueError) as excinfo:
        StoryFactory.create_story_builder(
            "UNSUPPORTED_GROUP", mock_query_service, mock_analysis_service, mock_analysis_manager, mock_db_session
        )
    assert str(excinfo.value) == "No story builder found for story group UNSUPPORTED_GROUP"


def test_story_factory_get_story_builder_growth():
    story_builder_class = StoryFactory.get_story_builder(StoryGroup.GROWTH_RATES)
    assert story_builder_class == GrowthStoryBuilder


def test_story_factory_get_story_builder_unsupported_group():
    with pytest.raises(ValueError) as excinfo:
        StoryFactory.get_story_builder("UNSUPPORTED_GROUP")

    assert excinfo.typename == "ValueError"


def test_story_factory_base_class():
    assert StoryFactory.base_class == StoryBuilderBase
