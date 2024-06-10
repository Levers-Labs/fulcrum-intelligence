from datetime import date

import pandas as pd
from fastapi import HTTPException

from analysis_manager.config import settings
from commons.clients.query_manager import QueryManagerClient
from fulcrum_core import AnalysisManager


class SegmentDriftService:
    def __init__(
        self,
        analysis_manager: AnalysisManager,
        query_manager: QueryManagerClient,
    ):
        self.analysis_manager = analysis_manager
        self.query_manager = query_manager

    async def get_segment_drift_data(
        self,
        metric_id,
        dimensions,
        evaluation_start_date: date,
        evaluation_end_date: date,
        comparison_start_date: date,
        comparison_end_date: date,
    ):

        evaluation_data = await self.query_manager.get_metric_time_series(
            metric_id=metric_id,
            start_date=evaluation_start_date,
            end_date=evaluation_end_date,
            dimensions=dimensions,
        )

        comparison_data = await self.query_manager.get_metric_time_series(
            metric_id=metric_id,
            start_date=comparison_start_date,
            end_date=comparison_end_date,
            dimensions=dimensions,
        )

        data_frame = pd.json_normalize(evaluation_data + comparison_data)

        invalid_dimensions = [col for col in dimensions if col not in data_frame.columns]

        if invalid_dimensions:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid dimensions : {', '.join(invalid_dimensions)}, for given metric {metric_id}",
            )

        return await self.analysis_manager.segment_drift(
            dsensei_base_url=settings.DSENSEI_BASE_URL,
            df=data_frame,
            evaluation_start_date=evaluation_start_date,
            evaluation_end_date=evaluation_end_date,
            comparison_start_date=comparison_start_date,
            comparison_end_date=comparison_end_date,
            dimensions=dimensions,
        )

    # separate dimension based slices,
    def get_dimension_slice_values_df(self, segment_drift_data, single_dimension=True):
        df = pd.DataFrame()
        for slice_info in segment_drift_data["dimension_slices"]:
            if single_dimension and len(slice_info["key"]) <= 1:
                continue

            dimension = slice_info["key"][0]["dimension"]
            slice_name = slice_info["key"][0]["value"]

            row = {
                "dimension": dimension,
                "slice": slice_name,
                "label": slice_info["serialized_key"],
                "evaluation_value": slice_info["evaluation_value"]["slice_value"],
                "comparison_value": slice_info["comparison_value"]["slice_value"],
            }

            df = pd.concat([df, pd.DataFrame(row, index=[0])], ignore_index=True)

        return df

    def get_top_or_bottom_n_segments(
        self,
        slice_data_frame: pd.DataFrame,
        top: bool = True,
        no_of_slices=4,
    ):
        if top:
            return slice_data_frame.iloc[:no_of_slices]
        else:
            return slice_data_frame.iloc[max(0, len(slice_data_frame) - no_of_slices) :]
