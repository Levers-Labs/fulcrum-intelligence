import logging
from datetime import date
from typing import Annotated, Any, Union

import numpy as np
import pandas as pd
from fastapi import (
    APIRouter,
    Body,
    HTTPException,
    Security,
)

from analysis_manager.config import get_settings
from analysis_manager.core.dependencies import (
    AnalysisManagerDep,
    ComponentDriftServiceDep,
    QueryManagerClientDep,
    oauth2_auth,
)
from analysis_manager.core.models import Component, ComponentDriftRequest
from analysis_manager.core.models.correlate import CorrelateRead
from analysis_manager.core.models.leverage_id import LeverageRequest, LeverageResponse
from analysis_manager.core.schema import (
    CorrelateRequest,
    DescribeRequest,
    DescribeResponse,
    ForecastRequest,
    ForecastResponse,
    InfluenceAttributionRequest,
    InfluenceAttributionResponse,
    ProcessControlRequest,
    ProcessControlResponse,
    SegmentDriftRequest,
    SegmentDriftResponse,
)
from analysis_manager.exceptions import ComplexValueError, MetricValueNotFoundError
from commons.auth.scopes import ANALYSIS_MANAGER_ALL
from fulcrum_core.execptions import InsufficientDataError
from fulcrum_core.modules import LeverageIdCalculator

router = APIRouter(prefix="/analyze", tags=["analyze"])
logger = logging.getLogger(__name__)


@router.post(
    "/describe",
    response_model=list[DescribeResponse],
    dependencies=[Security(oauth2_auth().verify, scopes=[ANALYSIS_MANAGER_ALL])],
)
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
                    "grain": "day",
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
    metric = await query_manager.get_metric(body.metric_id)

    metrics = await query_manager.get_metric_time_series(
        metric_id=str(body.metric_id),
        start_date=body.start_date,
        end_date=body.end_date,
        dimensions=dimensions,
        grain=body.grain,
    )
    if not metrics:
        return []

    # list of columns to include in df
    columns = ["metric_id", "date", "value"]
    for dimension in dimensions:
        columns.append(dimension)
        # prepare the input df
    df = pd.DataFrame(metrics, columns=columns)

    result_df = analysis_manager.describe(
        metric_id=body.metric_id,
        df=df,
        start_date=body.start_date,
        end_date=body.end_date,
        dimensions=dimensions,
        aggregation_function=metric.get("grain_aggregation") or "sum",  # noqa
    )
    # convert the result to a list of dictionaries
    results = result_df.to_dict(orient="records")
    return results


@router.post(
    "/correlate",
    response_model=list[CorrelateRead],
    dependencies=[Security(oauth2_auth().verify, scopes=[ANALYSIS_MANAGER_ALL])],
)
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

    metrics_df = pd.DataFrame(metric_values, columns=["metric_id", "date", "value"])  # noqa
    # return the correlation coefficient for each pair of metrics
    return analysis_manager.correlate(df=metrics_df)


@router.post(
    "/process-control",
    response_model=list[ProcessControlResponse],
    dependencies=[Security(oauth2_auth().verify, scopes=[ANALYSIS_MANAGER_ALL])],
)
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
        raise MetricValueNotFoundError(request.metric_id)

    # process control analysis
    try:
        result_df = analysis_manager.process_control(df=values_df)  # noqa
    except InsufficientDataError as e:
        logger.error(f"Insufficient data for process control analysis: {e}")
        raise HTTPException(status_code=400, detail=e.message) from e

    result_df.replace([np.inf, -np.inf], None, inplace=True)
    # convert the result to a list of dictionaries
    results = result_df.to_dict(orient="records")
    return results


@router.post(
    "/drift/component",
    response_model=Component,
    dependencies=[Security(oauth2_auth().verify, scopes=[ANALYSIS_MANAGER_ALL])],
)
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
        metric,  # noqa
        evaluation_start_date=drift_req.evaluation_start_date,
        evaluation_end_date=drift_req.evaluation_end_date,
        comparison_start_date=drift_req.comparison_start_date,
        comparison_end_date=drift_req.comparison_end_date,
    )


@router.post(
    "/forecast/simple",
    response_model=list[ForecastResponse],
    dependencies=[Security(oauth2_auth().verify, scopes=[ANALYSIS_MANAGER_ALL])],
)
async def simple_forecast(
    analysis_manager: AnalysisManagerDep,
    query_manager: QueryManagerClientDep,
    request: Annotated[
        ForecastRequest,
        Body(
            examples=[
                {
                    "metric_id": "NewBizDeals",
                    "start_date": "2024-01-01",
                    "end_date": "2024-04-30",
                    "grain": "day",
                    "forecast_horizon": 7,
                    "confidence_interval": 95,
                }
            ]
        ),
    ],
) -> Any:
    """
    Simple Forecast a metric.
    """
    # get metric values for the given metric_ids and date range
    values_df = await query_manager.get_metric_time_series_df(
        metric_id=request.metric_id, start_date=request.start_date, end_date=request.end_date, grain=request.grain
    )
    if values_df.empty:
        raise HTTPException(status_code=400, detail="No data found for the given metric")

    try:
        forecast_horizon = request.forecast_horizon
        forcast_till_date = request.forecast_till_date
        if forecast_horizon is None and forcast_till_date is None:
            raise HTTPException(
                status_code=400, detail="Either forecast_horizon or forecast_till_date should be provided"
            )
        res = analysis_manager.simple_forecast(
            df=values_df,
            grain=request.grain,  # type: ignore
            forecast_horizon=request.forecast_horizon,
            forecast_till_date=request.forecast_till_date,
            conf_interval=request.confidence_interval,
        )
    except InsufficientDataError as e:
        logger.error(f"Insufficient data for forecast analysis: {e}")
        raise HTTPException(status_code=400, detail=e.message) from e

    return res


@router.post(
    "/drift/segment",
    response_model=SegmentDriftResponse,
    dependencies=[Security(oauth2_auth().verify, scopes=[ANALYSIS_MANAGER_ALL])],
)
async def segment_drift(
    analysis_manager: AnalysisManagerDep,
    query_manager: QueryManagerClientDep,
    drift_req: Annotated[
        SegmentDriftRequest,
        Body(
            examples=[
                {
                    "metric_id": "NewBizDeals",
                    "evaluation_start_date": date(2025, 3, 1),
                    "evaluation_end_date": date(2025, 3, 30),
                    "comparison_start_date": date(2024, 3, 1),
                    "comparison_end_date": date(2024, 3, 30),
                    "dimensions": ["region", "stage_name"],
                }
            ]
        ),
    ],
):
    logger.debug(f"Segment drift request: {drift_req}")

    settings = get_settings()
    if len(drift_req.dimensions) == 0:
        raise HTTPException(status_code=400, detail="at least one dimension is required")

    evaluation_data = await query_manager.get_metric_time_series(
        metric_id=drift_req.metric_id,
        start_date=drift_req.evaluation_start_date,
        end_date=drift_req.evaluation_end_date,
        dimensions=drift_req.dimensions,
    )

    comparison_data = await query_manager.get_metric_time_series(
        metric_id=drift_req.metric_id,
        start_date=drift_req.comparison_start_date,
        end_date=drift_req.comparison_end_date,
        dimensions=drift_req.dimensions,
    )

    data_frame = pd.json_normalize(evaluation_data + comparison_data)

    invalid_dimensions = [col for col in drift_req.dimensions if col not in data_frame.columns]

    if invalid_dimensions:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid dimensions : {', '.join(invalid_dimensions)}, for given metric {drift_req.metric_id}",
        )

    return await analysis_manager.segment_drift(
        dsensei_base_url=settings.DSENSEI_BASE_URL,
        df=data_frame,
        evaluation_start_date=drift_req.evaluation_start_date,
        evaluation_end_date=drift_req.evaluation_end_date,
        comparison_start_date=drift_req.comparison_start_date,
        comparison_end_date=drift_req.comparison_end_date,
        dimensions=drift_req.dimensions,
    )


@router.post(
    "/influence-attribution",
    response_model=InfluenceAttributionResponse,
    dependencies=[Security(oauth2_auth().verify, scopes=[ANALYSIS_MANAGER_ALL])],
)
async def influence_attribution(
    analysis_manager: AnalysisManagerDep,
    query_manager: QueryManagerClientDep,
    request: Annotated[
        InfluenceAttributionRequest,
        Body(
            examples=[
                {
                    "metric_id": "NewBizDeals",
                    "start_date": "2022-01-01",
                    "end_date": "2023-12-31",
                    "grain": "week",
                    "evaluation_start_date": "2024-01-01",
                    "evaluation_end_date": "2024-01-31",
                    "comparison_start_date": "2024-02-01",
                    "comparison_end_date": "2024-02-29",
                }
            ]
        ),
    ],
):
    """
    Analyze influence attribution for a given metric.
    """
    logger.debug(f"Influence attribution request: {request}")
    metric = await query_manager.get_metric(request.metric_id)

    # check if metric has influeces ie. influenced_by
    influenced_by = metric.get("influenced_by", [])  # noqa
    if not influenced_by:
        raise HTTPException(status_code=400, detail="Metric is not influenced by any other metric.")

    # get metric time series df for output metric
    df = await query_manager.get_metric_time_series_df(
        metric_id=request.metric_id,
        start_date=request.start_date,
        end_date=request.end_date,
        grain=request.grain,
    )
    # get the time series for all the input metric
    inputs_dfs = []
    for metric_id in influenced_by:
        inputs_df = await query_manager.get_metric_time_series_df(
            metric_id=metric_id,
            start_date=request.start_date,
            end_date=request.end_date,
            grain=request.grain,
        )
        inputs_dfs.append(inputs_df)

    # get the evaluation and comparison values for all metrics
    metric_ids = [request.metric_id] + influenced_by
    eval_values = await query_manager.get_metrics_value(
        metric_ids=metric_ids, start_date=request.evaluation_start_date, end_date=request.evaluation_end_date
    )
    comp_values = await query_manager.get_metrics_value(
        metric_ids=metric_ids, start_date=request.comparison_start_date, end_date=request.comparison_end_date
    )
    # prepare the values for the influence attribution analysis
    values: list[dict[str, Any]] = []
    for metric_id in metric_ids:
        values.append(
            {
                "metric_id": metric_id,
                "evaluation_value": eval_values[metric_id],
                "comparison_value": comp_values[metric_id],
            }
        )
    # run the influence attribution analysis
    expression, influence = analysis_manager.influence_attribution(
        df=df,
        input_dfs=inputs_dfs,
        metric_id=request.metric_id,
        values=values,
    )
    return {
        "expression": expression,
        "influence": influence,
    }


@router.post(
    "/analysis/leverage_id",
    response_model=Union[LeverageResponse, None],
    dependencies=[Security(oauth2_auth().verify, scopes=[ANALYSIS_MANAGER_ALL])],
)
async def leverage_id(
    query_manager: QueryManagerClientDep,
    request: Annotated[
        LeverageRequest,
        Body(
            examples=[
                {
                    "metric_id": "NewBizDeals",
                    "start_date": "2024-02-01",
                    "end_date": "2024-03-01",
                    "grain": "month",
                }
            ]
        ),
    ],
) -> Any:
    """
    Analyze LeverageResponse ID for a given metric.
    """
    logger.debug(f"LeverageResponse Id request: {request}")
    expr = None
    # Fetch the metric details from the query manager
    metric = await query_manager.get_metric(request.metric_id)
    # Extract the metric expression from the fetched metric details
    expr = metric.get("metric_expression", None)

    if expr is None:
        # Raise an HTTP 404 error if the metric expression is not found
        raise HTTPException(status_code=400, detail=f"Metric expression not found for metric_id: {request.metric_id}")

    # Get the nested expressions for the metric and the list of metric IDs involved
    metric_expression, metric_ids = await query_manager.get_expressions(request.metric_id, nested=True)

    # Fetch the maximum values for the metrics involved
    max_values = await query_manager.get_metrics_max_values(metric_ids)

    # Fetch the time series data for the metrics within the specified date range and grain
    values_df = await query_manager.get_metrics_time_series_df(
        metric_ids=metric_ids, start_date=request.start_date, end_date=request.end_date, grain=request.grain
    )

    try:
        # Initialize the LeverageIdCalculator with the metric expression and max values
        leverage_calculator = LeverageIdCalculator(metric_expression, max_values)  # type: ignore

        # Run the leverage calculator with the time series data and get the result
        result = leverage_calculator.run(values_df)
        # Return the calculated leverage ID result
        return result
    except TypeError as t:
        raise ComplexValueError(request.metric_id) from t
