import datetime
from typing import Annotated, Any, List, Optional

from fastapi import APIRouter, Body

from app.core.schemas import Metric
from app.core.types import Dimension

router = APIRouter(prefix="/metrics", tags=["metrics"])


@router.post("/values", response_model=list[Metric])
async def read_metric(
    metric_ids: List[int],
    start_date: Annotated[datetime.date, Body()],
    end_date: Annotated[datetime.date, Body()],
    dimensions: Optional[List[Dimension]]=None
) -> Any:
    print("metric_ids: ", metric_ids)
    print("start_date: ", start_date)
    print("end_date: ", end_date)
    print("dimensions: ", dimensions)
    metrics = [
        {
            "id": 1,
            "date": "1958-01-04",
            "name": "Count of Closed Opportunities",
            "value": 1,
            "dimension": "Creating Org",
            "slice": "Other",
        },
        {
            "id": 1,
            "date": "1958-01-04",
            "name": "Count of Closed Opportunities",
            "value": 1,
            "dimension": "Geosegmentation",
            "slice": "Rest of World",
        },
        {
            "id": 1,
            "date": "1958-01-04",
            "name": "Count of Closed Opportunities",
            "value": 1,
            "dimension": "Opportunity Source",
            "slice": "Agency",
        },
        {
            "id": 1,
            "date": "1958-01-04",
            "name": "Count of Closed Opportunities",
            "value": 1,
            "dimension": "Owning Org",
            "slice": "Other",
        },
        {
            "id": 1,
            "date": "1958-01-04",
            "name": "Count of Closed Opportunities",
            "value": 1,
            "dimension": "Probability to Close Tier",
            "slice": "High",
        },
        {
            "id": 1,
            "date": "1999-10-30",
            "name": "Count of Closed Opportunities",
            "value": 3,
            "dimension": "Creating Org",
            "slice": "Other",
        },
        {
            "id": 1,
            "date": "1999-10-30",
            "name": "Count of Closed Opportunities",
            "value": 3,
            "dimension": "Geosegmentation",
            "slice": "Americas",
        },
        {
            "id": 1,
            "date": "1999-10-30",
            "name": "Count of Closed Opportunities",
            "value": 3,
            "dimension": "Opportunity Source",
            "slice": "Other",
        },
        {
            "id": 1,
            "date": "1999-10-30",
            "name": "Count of Closed Opportunities",
            "value": 3,
            "dimension": "Owning Org",
            "slice": "Other",
        },
        {
            "id": 1,
            "date": "1999-10-30",
            "name": "Count of Closed Opportunities",
            "value": 3,
            "dimension": "Probability to Close Tier",
            "slice": "Low",
        },
        {
            "id": 1,
            "date": "2000-01-04",
            "name": "Count of Closed Opportunities",
            "value": 1,
            "dimension": "Creating Org",
            "slice": "Other",
        },
        {
            "id": 1,
            "date": "2000-01-04",
            "name": "Count of Closed Opportunities",
            "value": 1,
            "dimension": "Geosegmentation",
            "slice": "Americas",
        },
        {
            "id": 1,
            "date": "2000-01-04",
            "name": "Count of Closed Opportunities",
            "value": 1,
            "dimension": "Opportunity Source",
            "slice": "Agency",
        },
        {
            "id": 1,
            "date": "2000-01-04",
            "name": "Count of Closed Opportunities",
            "value": 1,
            "dimension": "Owning Org",
            "slice": "Other",
        },
        {
            "id": 1,
            "date": "2000-01-04",
            "name": "Count of Closed Opportunities",
            "value": 1,
            "dimension": "Probability to Close Tier",
            "slice": "Low",
        },
        {
            "id": 1,
            "date": "2006-04-28",
            "name": "Count of Closed Opportunities",
            "value": 1,
            "dimension": "Creating Org",
            "slice": "Other",
        },
    ]
    return metrics
