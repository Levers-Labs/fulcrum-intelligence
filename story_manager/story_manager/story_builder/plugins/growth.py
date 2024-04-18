import logging

from commons.models.enums import Granularity
from story_manager.core.enums import StoryGenre
from story_manager.story_builder import StoryBuilderBase

logger = logging.getLogger(__name__)


class GrowthStoryBuilder(StoryBuilderBase):
    genre = StoryGenre.GROWTH  # type: ignore
    supported_grains = [Granularity.DAY, Granularity.WEEK, Granularity.MONTH, Granularity.QUARTER]

    def generate_stories(self, metric_id: str, grain: Granularity) -> list[dict]:
        """
        Generate growth stories for the given metric and grain

        :param metric_id: The metric ID for which growth stories are generated
        :param grain: The grain for which growth stories are generated
        :return: A list of generated growth stories
        """
        return []
