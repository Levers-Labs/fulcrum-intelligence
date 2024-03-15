import datetime
import json
import logging
from typing import Annotated, Any, List, Optional

import aiofiles
import pandas as pd
from fastapi import APIRouter, Body

from app.core.schemas import DimensionFilter, Metric

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/metrics", tags=["metrics"])


@router.post("/values", response_model=list[Metric])
async def read_metric_values(
    metric_ids: list[int],
    start_date: Annotated[datetime.date, Body()],
    end_date: Annotated[datetime.date, Body()],
    dimensions: Annotated[
        list[DimensionFilter] | None,
        Body(
            examples=[
                [
                    {
                        "dimension": "Geosegmentation",
                        "slices": ["Rest of World", "other"],
                    },
                    {
                        "dimension": "Creating Org",
                        "slices": [],
                    },
                ]
            ]
        ),
    ] = None,
) -> Any:
    logger.debug("metric_ids: ", metric_ids)
    logger.debug("start_date: ", start_date)
    logger.debug("end_date: ", end_date)
    logger.debug("dimensions: ", dimensions)

    async with aiofiles.open("app/core/data/metric_values.json") as f:
        contents = await f.read()
    metrics = json.loads(contents)

    # prepare df and filter
    metrics_df = pd.DataFrame(metrics)

    metrics_df["date"] = pd.to_datetime(metrics_df["date"]).dt.date

    # filter by metric ids
    metrics_df = metrics_df[metrics_df["id"].isin(metric_ids)]
    metrics_df = metrics_df[(metrics_df["date"] >= start_date) & (metrics_df["date"] <= end_date)]

    # filter by dimensions
    if not dimensions:
        return metrics_df.to_dict(orient="records")

    result_df = pd.DataFrame()
    for dimension in dimensions:
        slices = dimension.slices
        _df = metrics_df[metrics_df["dimension"] == dimension.dimension]
        if slices:
            _df = metrics_df[metrics_df["slice"].isin(slices)]
        result_df = pd.concat([result_df, _df])

    return result_df.to_dict(orient="records")
