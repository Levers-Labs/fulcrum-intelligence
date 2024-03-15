from typing import Annotated, Any

import pandas as pd
from fastapi import APIRouter, Body

from app.core.dependencies import (
    AnalysisManagerDep,
    QueryManagerClientDep,
    UsersCRUDDep,
)
from app.core.models import User, UserRead
from app.core.models.correlate import CorrelateRead
from app.core.schema import (
    CorrelateRequest,
    DescribeRequest,
    DescribeResponse,
    UserList,
)

router = APIRouter(prefix="/analyze", tags=["analyze"])
user_router = APIRouter(prefix="/users", tags=["users"])


@user_router.get("", response_model=UserList)
async def list_users(users: UsersCRUDDep, offset: int = 0, limit: int = 100) -> Any:
    """
    Retrieve users.
    """
    count = await users.total_count()
    users: list[User] = await users.list(offset=offset, limit=limit)
    return UserList(results=users, count=count)


@user_router.get("/{user_id}", response_model=UserRead)
async def get_user(user_id: int, users: UsersCRUDDep) -> Any:
    """
    Retrieve a user by ID.
    """
    user: User = await users.get(user_id)
    return user


@router.post("/describe", response_model=list[DescribeResponse])
async def describe_analysis(
    analysis_manager: AnalysisManagerDep,
    query_manager: QueryManagerClientDep,
    body: Annotated[
        DescribeRequest,
        Body(
            examples=[
                {
                    "metric_id": 5,
                    "start_date": "2024-01-01",
                    "end_date": "2024-04-30",
                    "dimensions": [
                        {
                            "dimension": "Geosegmentation",
                            "slices": ["Rest of World", "other"],
                        },
                        {
                            "dimension": "Creating Org",
                            "slices": [],
                        },
                    ],
                }
            ]
        ),
    ],
) -> Any:
    """
    Describe an analysis.
    """
    metrics = await query_manager.get_metric_values(
        metric_ids=[body.metric_id], start_date=body.start_date, end_date=body.end_date, dimensions=body.dimensions
    )
    if not metrics:
        return []
    # metrics to dataframe
    metrics_df = pd.DataFrame(metrics)
    # column names mapping
    columns = {
        "id": "METRIC_ID",
        "date": "DAY",
        "value": "METRIC_VALUE",
        "dimension": "DIMENSION_NAME",
        "slice": "SLICE",
    }
    # rename columns
    metrics_df.rename(columns=columns, inplace=True)
    metrics_df["DAY"] = pd.to_datetime(metrics_df["DAY"], format="%Y-%m-%d")
    metrics_df["METRIC_ID"] = metrics_df["METRIC_ID"].astype(str)
    return analysis_manager.describe(data=metrics_df)


@router.post("/correlate", response_model=list[CorrelateRead])
async def correlate(
    analysis_manager: AnalysisManagerDep,
    query_manager: QueryManagerClientDep,
    correlate_request: Annotated[
        CorrelateRequest,
        Body(examples=[{"metric_ids": ["5", "6"], "start_date": "2024-01-01", "end_date": "2024-04-30"}]),
    ],
) -> Any:
    """
    Analyze correlations between metrics.
    """
    # get metric values for the given metric_ids and date range
    metrics = await query_manager.get_metric_values(
        metric_ids=correlate_request.metric_ids,
        start_date=correlate_request.start_date,
        end_date=correlate_request.end_date,
    )
    if not metrics:
        return []
    # calculate correlation coefficient using a fulcrum core library
    # metrics to dataframe
    metrics_df = pd.DataFrame(metrics)
    # column names mapping
    columns = {"id": "METRIC_ID", "date": "DAY", "value": "METRIC_VALUE"}
    # rename columns
    metrics_df.rename(columns=columns, inplace=True)
    # only keep the required columns
    metrics_df = metrics_df[["METRIC_ID", "DAY", "METRIC_VALUE"]]
    metrics_df["METRIC_ID"] = metrics_df["METRIC_ID"].astype(str)

    # return the correlation coefficient for each pair of metrics
    return analysis_manager.correlate(
        data=metrics_df, start_date=correlate_request.start_date, end_date=correlate_request.end_date
    )
