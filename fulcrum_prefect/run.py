import asyncio
import logging

from fulcrum_prefect.gen_stories import generate_stories

logger = logging.getLogger(__name__)
if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        logger.error("Please provide a group name as argument")
        sys.exit(1)

    group = sys.argv[1]
    asyncio.run(generate_stories(group))  # type: ignore
