import asyncio
import logging

from tasks_manager.flows.stories import generate_stories

logger = logging.getLogger(__name__)
if __name__ == "__main__":
    asyncio.run(generate_stories())  # type: ignore
