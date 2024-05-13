import logging
from typing import Annotated, Any

import pandas as pd
from fastapi import (
    APIRouter,
    Body,
    Depends,
    HTTPException,
)

from analysis_manager.core.dependencies import (
    AnalysisManagerDep,
    ComponentDriftServiceDep,
    QueryManagerClientDep,
    UsersCRUDDep,
)
from analysis_manager.core.models import (
    Component,
    ComponentDriftRequest,
    User,
    UserRead,
)
from analysis_manager.core.models.correlate import CorrelateRead
from analysis_manager.core.schema import (
    CorrelateRequest,
    DescribeRequest,
    DescribeResponse,
    ProcessControlRequest,
    ProcessControlResponse,
    UserList,
)
from commons.utilities.pagination import PaginationParams
from fulcrum_core.execptions import InsufficientDataError

router = APIRouter(prefix="/analyze", tags=["analyze"])
user_router = APIRouter(prefix="/users", tags=["users"])
logger = logging.getLogger(__name__)


@user_router.get("", response_model=UserList)
async def list_users(
    users: UsersCRUDDep,
    params: Annotated[PaginationParams, Depends(PaginationParams)],
) -> Any:
    """
    Retrieve users.
    """
    count = await users.total_count()
    results: list[UserRead] = [UserRead.from_orm(user) for user in await users.list_results(params=params)]
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
                    "metric_id": "NewBizDeals",
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
        metric_id=str(body.metric_id), start_date=body.start_date, end_date=body.end_date, dimensions=dimensions
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
        Body(
            examples=[
                {
                    "metric_ids": ["NewBizDeals", "OpenNewBizOpps"],
                    "start_date": "2024-01-01",
                    "end_date": "2024-04-30",
                    "grain": "day",
                }
            ]
        ),
    ],
) -> Any:
    """
    Analyze correlations between metrics.
    """
    # get metric values for the given metric_ids and date range
    metric_values = await query_manager.get_metrics_time_series(
        metric_ids=correlate_request.metric_ids,
        start_date=correlate_request.start_date,
        end_date=correlate_request.end_date,
        grain=correlate_request.grain,
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
        Body(examples=[{"metric_id": "NewMRR", "start_date": "2024-01-01", "end_date": "2024-04-30", "grain": "day"}]),
    ],
) -> Any:
    values_df = await query_manager.get_metric_time_series_df(
        metric_id=request.metric_id,
        start_date=request.start_date,
        end_date=request.end_date,
        grain=request.grain,
    )
    if values_df.empty:
        raise HTTPException(status_code=400, detail="No data found for the given metric")

    # process control analysis
    try:
        result_df = analysis_manager.process_control(df=values_df)
    except InsufficientDataError as e:
        logger.error(f"Insufficient data for process control analysis: {e}")
        raise HTTPException(status_code=400, detail=e.message) from e

    # convert the result to a list of dictionaries
    results = result_df.to_dict(orient="records")
    return results


@router.post("/drift/component", response_model=Component)
async def component_drift(
    query_manager: QueryManagerClientDep,
    component_drift_service: ComponentDriftServiceDep,
    drift_req: Annotated[
        ComponentDriftRequest,
        Body(
            examples=[
                {
                    "metric_id": "NewBizDeals",
                    "evaluation_start_date": "2024-02-01",
                    "evaluation_end_date": "2024-03-01",
                    "comparison_start_date": "2024-01-01",
                    "comparison_end_date": "2024-02-01",
                }
            ]
        ),
    ],
) -> Any:
    """
    Analyze component drift for a given metric.
    """
    logger.debug(f"Component drift request: {drift_req}")
    metric = await query_manager.get_metric(drift_req.metric_id)
    return await component_drift_service.calculate_drift(
        metric,
        evaluation_start_date=drift_req.evaluation_start_date,
        evaluation_end_date=drift_req.evaluation_end_date,
        comparison_start_date=drift_req.comparison_start_date,
        comparison_end_date=drift_req.comparison_end_date,
    )
