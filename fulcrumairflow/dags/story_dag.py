import logging
from datetime import datetime

from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from sqlalchemy.ext.asyncio import AsyncSession

from commons.clients.analysis_manager import AnalysisManagerClient
from commons.clients.query_manager import QueryManagerClient
from fulcrum_core import AnalysisManager
from story_manager.core.enums import StoryGroup
from story_manager.story_builder import StoryManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Fetch the story group list from the enum
story_group_list = [group.value for group in StoryGroup]


def run_stories(group):
    try:
        logger.info(f"Execution started for story group: {group}")
        query_service = QueryManagerClient()
        analysis_service = AnalysisManagerClient()
        analysis_manager = AnalysisManager()
        db_session = AsyncSession()

        story_manager = StoryManager(
            query_service=query_service,
            analysis_service=analysis_service,
            analysis_manager=analysis_manager,
            db_session=db_session,
        )
        story_manager.run_all_builders_by_group(group)
        logger.info("Story generation completed.")
    except Exception as ex:
        logger.error(f"Error occurred while generating story for {group}: {ex}")
        raise ex


# Dynamic DAG generation
for story_group in story_group_list:
    dag_id = f"fulcrum_{story_group}_story_dag"

    @dag(dag_id=dag_id, start_date=datetime(2024, 7, 1), schedule_interval="*/30 * * * *", catchup=False)
    def dynamic_generated_dag(group):
        task_id = f"generate_story_{group}"
        PythonOperator(
            task_id=task_id,
            python_callable=run_stories,
            op_args=[group],
            dag=dynamic_generated_dag,
        )

    dynamic_generated_dag(story_group)
