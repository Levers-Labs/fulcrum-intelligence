from typing import Annotated, Any

import pandas as pd
from fastapi import APIRouter, Body

from analysis_manager.core.dependencies import AnalysisManagerDep, QueryManagerClientDep, UsersCRUDDep
from analysis_manager.core.models import User, UserRead
from analysis_manager.core.models.correlate import CorrelateRead
from analysis_manager.core.schema import (
    CorrelateRequest,
    DescribeRequest,
    DescribeResponse,
    ProcessControlRequest,
    ProcessControlResponse,
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
    results: list[UserRead] = [UserRead.from_orm(user) for user in await users.list(offset=offset, limit=limit)]
    return UserList(results=results, count=count)


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
                            "members": ["Rest of World", "other"],
                        },
                        {
                            "dimension": "Creating Org",
                            "members": [],
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
    dimensions = [dimension.dimension for dimension in body.dimensions] if body.dimensions else []
    metrics = await query_manager.get_metric_values(
        metric_ids=[str(body.metric_id)], start_date=body.start_date, end_date=body.end_date, dimensions=dimensions
    )
    if not metrics:
        return []

    return analysis_manager.describe(
        metric_id=body.metric_id,
        data=metrics,
        start_date=pd.Timestamp(body.start_date),
        end_date=pd.Timestamp(body.end_date),
        dimensions=dimensions,
        aggregation_function="sum",
    )


@router.post("/correlate", response_model=list[CorrelateRead])
async def correlate(
    analysis_manager: AnalysisManagerDep,
    query_manager: QueryManagerClientDep,
    correlate_request: Annotated[
        CorrelateRequest,
        Body(examples=[{"metric_ids": ["NewMRR", "CAC"], "start_date": "2024-01-01", "end_date": "2024-04-30"}]),
    ],
) -> Any:
    """
    Analyze correlations between metrics.
    """
    # get metric values for the given metric_ids and date range
    metric_values = await query_manager.get_metric_values(
        metric_ids=correlate_request.metric_ids,
        start_date=correlate_request.start_date,
        end_date=correlate_request.end_date,
    )
    if not metric_values:
        return []
    metrics_df = pd.DataFrame(metric_values, columns=["metric_id", "date", "value"])
    columns = {"metric_id": "METRIC_ID", "date": "DAY", "value": "METRIC_VALUE"}
    metrics_df.rename(columns=columns, inplace=True)
    metrics_df["METRIC_ID"] = metrics_df["METRIC_ID"].astype(str)

    # return the correlation coefficient for each pair of metrics
    return analysis_manager.correlate(
        data=metrics_df, start_date=correlate_request.start_date, end_date=correlate_request.end_date
    )


@router.post("/process-control", response_model=list[ProcessControlResponse])
async def process_control(
    analysis_manager: AnalysisManagerDep,
    query_manager: QueryManagerClientDep,
    request: Annotated[
        ProcessControlRequest,
        Body(
            examples=[
                {"metric_id": "NewMRR", "start_date": "2024-01-01", "end_date": "2024-04-30", "grains": ["day", "week"]}
            ]
        ),
    ],
) -> Any:
    metric_values = await query_manager.get_metric_values(
        metric_ids=[request.metric_id],
        start_date=request.start_date,
        end_date=request.end_date,
    )
    if not metric_values:
        return []
    metrics_df = pd.DataFrame(metric_values, columns=["metric_id", "date", "value"])
    columns = {"metric_id": "METRIC_ID", "date": "DAY", "value": "METRIC_VALUE"}
    metrics_df.rename(columns=columns, inplace=True)
    metrics_df["METRIC_ID"] = metrics_df["METRIC_ID"].astype(str)
    results: list[dict] = []
    for grain in request.grains:
        metrics_df["GRAIN"] = metrics_df["DAY"].astype(str)
        metrics_df = metrics_df[["METRIC_VALUE", "GRAIN"]]
        results.extend(
            analysis_manager.process_control(
                data=metrics_df,
                metric_id=request.metric_id,
                start_date=pd.Timestamp(request.start_date),
                end_date=pd.Timestamp(request.end_date),
                grain=grain.value,
            )
        )
    res_df = pd.DataFrame(results)
    columns = {
        "METRIC_ID": "metric_id",
        "DATE": "date",
        "METRIC_VALUE": "metric_value",
        "GRAIN": "grain",
        "CENTRAL_LINE": "central_line",
        "UCL": "ucl",
        "LCL": "lcl",
    }
    res_df.rename(columns=columns, inplace=True)
    res_df["metric_id"] = request.metric_id
    res_df["start_date"] = request.start_date
    res_df["end_date"] = request.end_date

    return res_df.to_dict(orient="records")
