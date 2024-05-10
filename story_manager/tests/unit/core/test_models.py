import pytest
from pydantic import ValidationError

from commons.models.enums import Granularity
from story_manager.core.enums import (
    GROUP_TO_STORY_TYPE_MAPPING,
    StoryGenre,
    StoryGroup,
    StoryType,
)
from story_manager.core.models import Story


@pytest.mark.parametrize("group, story_type", [(StoryGroup.GROWTH_RATES, StoryType.ACCELERATING_GROWTH)])
def test_invalid_type_genre_combination(group, story_type):
    data = {
        "story_group": group,
        "story_type": story_type,
        "metric_id": "test_metric",
        "description": "Test Story",
        "template": "This is a test story.",
        "text": "This is a test story.",
    }
    with pytest.raises(ValidationError):
        Story.model_validate(data)


@pytest.mark.parametrize(
    "group, story_type",
    [(group, story_type) for group, valid_types in GROUP_TO_STORY_TYPE_MAPPING.items() for story_type in valid_types],
)
def test_valid_type_group_combination(group, story_type):
    data = {
        "genre": StoryGenre.GROWTH,
        "story_group": group,
        "grain": Granularity.DAY,
        "story_type": story_type,
        "metric_id": "test_metric",
        "description": "Test Story",
        "template": "This is a test story.",
        "text": "This is a test story.",
    }
    validated_data = Story.model_validate(data)
    assert validated_data.story_group == group
    assert validated_data.story_type == story_type
