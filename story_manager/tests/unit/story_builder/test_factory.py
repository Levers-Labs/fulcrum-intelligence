import pytest

from story_manager.story_builder import StoryBuilderBase
from story_manager.story_builder.factory import StoryFactory
from story_manager.story_builder.plugins.growth import GrowthStoryBuilder
from story_manager.story_manager.story_builder.core.enums import StoryGenre


def test_story_factory_create_story_builder_growth(mock_query_service, mock_analysis_service, mock_db_session):
    story_builder = StoryFactory.create_story_builder(
        StoryGenre.GROWTH, mock_query_service, mock_analysis_service, mock_db_session
    )
    assert isinstance(story_builder, GrowthStoryBuilder)


def test_story_factory_create_story_builder_unsupported_genre(
    mock_query_service, mock_analysis_service, mock_db_session
):
    with pytest.raises(ValueError) as excinfo:
        StoryFactory.create_story_builder(
            "UNSUPPORTED_GENRE", mock_query_service, mock_analysis_service, mock_db_session
        )
    assert str(excinfo.value) == "No story builder found for genre UNSUPPORTED_GENRE"


def test_story_factory_get_story_builder_growth():
    story_builder_class = StoryFactory.get_story_builder(StoryGenre.GROWTH)
    assert story_builder_class == GrowthStoryBuilder


def test_story_factory_get_story_builder_unsupported_genre():
    with pytest.raises(ValueError) as excinfo:
        StoryFactory.get_story_builder("UNSUPPORTED_GENRE")
    assert str(excinfo.value) == "No story builder found for genre UNSUPPORTED_GENRE"


def test_story_factory_base_class():
    assert StoryFactory.base_class == StoryBuilderBase
