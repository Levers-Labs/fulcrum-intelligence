import asyncio
import logging

from levers.models import Granularity
from tasks_manager.flows.pattern_analysis import trigger_run_pattern

logger = logging.getLogger(__name__)
if __name__ == "__main__":
    asyncio.run(
        trigger_run_pattern(  # type: ignore
            tenant_id=1,
            pattern_name="dimension_analysis",
            metric_id="newInqs",
            grain=Granularity.MONTH,
            dimension_name="region",
        )
    )
