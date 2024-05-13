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
        "title": "d/d growth is speeding up",
        "title_template": "{{pop}} growth is speeding up",
        "detail": "The d/d growth rate for NewBizDeals is speeding up. It is currently 15% and up from the 10% "
        "average over the past 11 days.",
        "detail_template": "The {{pop}} growth rate for {{metric.label}} is speeding up. It is currently {{"
        "current_growth}}% and up from the {{reference_growth}}% average over the past {{"
        "reference_period_days}} {{days}}s.",
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
        "title": "d/d growth is speeding up",
        "title_template": "{{pop}} growth is speeding up",
        "detail": "The d/d growth rate for NewBizDeals is speeding up. It is currently 15% and up from the 10% "
        "average over the past 11 days.",
        "detail_template": "The {{pop}} growth rate for {{metric.label}} is speeding up. It is currently {{"
        "current_growth}}% and up from the {{reference_growth}}% average over the past {{"
        "reference_period_days}} {{days}}s.",
    }
    validated_data = Story.model_validate(data)
    assert validated_data.story_group == group
    assert validated_data.story_type == story_type
