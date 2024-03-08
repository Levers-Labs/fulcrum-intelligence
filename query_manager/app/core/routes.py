from typing import Any

from fastapi import APIRouter

from app.core.schemas import Metric

router = APIRouter(prefix="/metrics", tags=["metrics"])


@router.get("/{metric_id}", response_model=Metric)
async def read_metric(metric_id: int) -> Any:
    return {"id": metric_id, "name": "Test Metric"}
