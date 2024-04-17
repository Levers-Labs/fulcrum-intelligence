import pytest
from pydantic import ValidationError

from story_manager.core.enums import GENRE_TO_STORY_TYPE_MAPPING, StoryGenre, StoryType
from story_manager.core.models import Story


@pytest.mark.parametrize(
    "genre, story_type",
    [(genre, story_type) for genre, valid_types in GENRE_TO_STORY_TYPE_MAPPING.items() for story_type in valid_types],
)
def test_valid_type_genre_combination(genre, story_type):
    data = {
        "genre": genre,
        "story_type": story_type,
        "metric_id": "test_metric",
        "description": "Test Story",
        "template": "This is a test story.",
        "text": "This is a test story.",
    }
    story = Story.model_validate(data)
    assert story.genre == genre
    assert story.story_type == story_type


@pytest.mark.parametrize("genre, story_type", [(StoryGenre.GROWTH, StoryType.SEGMENT_DRIFT)])
def test_invalid_type_genre_combination(genre, story_type):
    data = {
        "genre": genre,
        "story_type": story_type,
        "metric_id": "test_metric",
        "description": "Test Story",
        "template": "This is a test story.",
        "text": "This is a test story.",
    }
    with pytest.raises(ValidationError):
        Story.model_validate(data)
