import logging

from story_manager.story_manager.story_builder import StoryFactory
from story_manager.story_manager.story_builder.core.enums import StoryGenre

logger = logging.getLogger(__name__)


class StoryManager:
    """
    Class for managing and running story generation builders
    """

    def __init__(self, query_service, analysis_service, db_session):
        """
        Initialize the StoryManager instance

        :param query_service: QueryService instance for retrieving data
        :param analysis_service: AnalysisService instance for performing analysis
        :param db_session: Database session for persisting stories
        """
        self.query_service = query_service
        self.analysis_service = analysis_service
        self.db_session = db_session

    def run_all_builders(self) -> None:
        """
        Run all story generation builders
        """
        metrics = self.query_service.list_metrics()
        logger.info(f"Retrieved {len(metrics)} metrics from the query service")

        for genre in StoryGenre.__members__.values():
            logger.info(f"Running story builders for genre: {genre}")
            story_builder = StoryFactory.create_story_builder(
                genre, self.query_service, self.analysis_service, self.db_session
            )
            self._run_builder_for_metrics(story_builder, metrics)

    def _run_builder_for_metrics(self, story_builder, metrics: list[dict]) -> None:
        """
        Run the story builder for the given list of metrics

        :param story_builder: The story builder instance
        :param metrics: The list of metrics to generate stories for
        """
        total_metrics = len(metrics)
        for i, metric in enumerate(metrics, start=1):
            metric_id = metric["id"]
            logger.info(f"Processing metric {i}/{total_metrics}: ID: {metric_id}")
            for grain in story_builder.supported_grains:
                logger.info(f"Generating stories for grain: {grain}")
                try:
                    story_builder.run(metric["id"], grain)
                except Exception as e:
                    logger.exception(f"Error generating stories for metric {metric_id} with grain {grain}: {str(e)}")
                    continue
