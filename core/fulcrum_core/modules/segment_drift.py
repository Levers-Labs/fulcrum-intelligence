import os
import tempfile
import urllib
import uuid
from datetime import datetime
from typing import Any

import httpx
import pandas as pd
from sqlalchemy.exc import InvalidRequestError

from fulcrum_core.enums import MetricChangeDirection, TargetAim


class SegmentDriftEvaluator:
    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

    def __init__(self, dsensei_base_url: str):
        self.DSENSEI_BASE_URL = dsensei_base_url
        self.PATH_FOR_CSV = tempfile.gettempdir()

    async def calculate_segment_drift(
        self,
        df: pd.DataFrame,
        evaluation_start_date: str,
        evaluation_end_date: str,
        comparison_start_date: str,
        comparison_end_date: str,
        dimensions: list[str],
        metric_id: str = "value",
        date_column: str = "date",
        aggregation_option: str = "sum",
        aggregation_method: str = "SUM",
        target_metric_direction: str = "increasing",
    ):
        """
        This is the entrypoint for calculating the segment drift in the given data.
        Steps:
        1. we are converting the dataframe to csv file as dsensei server uses csv files as its data source.
        2. Converted CSV file will be sent to dsensei server, in response we will get the ID of the file.
        3. We will send the insight request to dsensei server to calculate the segment drift.

        """
        self.validate_request_data(
            df, aggregation_option=aggregation_option, aggregation_method=aggregation_method, metric_column=metric_id
        )

        csv_path = self.write_dataframe_to_csv(df)  # convert it to async

        # Uploading the file to dsensei server, as it works with CSV files only.
        file_id = await self.send_file_to_dsensei(csv_path)

        # removing the generated file
        if os.path.exists(csv_path):
            os.remove(csv_path)

        # fetching the insights of our data from dsensei server, get_insights
        result = await self.get_insights(
            file_id,
            evaluation_start_date,
            evaluation_end_date,
            comparison_start_date,
            comparison_end_date,
            dimensions,
            metric_id,
            date_column,
            aggregation_option,
            aggregation_method,
        )
        # main_key is the parent key of the whole result, its format is "metricColumn_aggregationMethod"
        # EX: "value_SUM"  or "region_DISTINCT"
        main_key = f"{metric_id}_{aggregation_method}"

        # dimension slice info consist of all calculations for all permutations of the provided dimensions
        dimension_slice_info = result[main_key]["dimensionSliceInfo"]
        overall_change = self.get_overall_change(result[main_key]["comparisonValue"], result[main_key]["baselineValue"])

        # processing the result to calculate the direction of metric change, whether its UPWARD DOWNWARD or UNCHANGED
        for segment in dimension_slice_info.keys():
            relative_change = self.calculate_segment_relative_change(segment, overall_change, dimension_slice_info)

            dimension_slice_info[segment]["relative_change"] = relative_change

            dimension_slice_info[segment]["pressure"] = self.get_metric_change_direction(
                relative_change, target_metric_direction
            )

        return result

    def get_overall_change(self, comparison_value, baseline_value):
        try:
            overall_change = (comparison_value - baseline_value) / baseline_value
        except ZeroDivisionError:
            overall_change = 0.0
        return overall_change

    def calculate_segment_relative_change(self, segment: str, overall_change: float, dimension_slice_info: dict):
        try:
            slice_comparison_value = dimension_slice_info[segment]["comparisonValue"]["sliceValue"]
        except KeyError:
            slice_comparison_value = 0

        try:
            slice_baseline_value = dimension_slice_info[segment]["baselineValue"]["sliceValue"]
        except KeyError:
            slice_baseline_value = 0

        try:
            change = (slice_comparison_value - slice_baseline_value) / slice_baseline_value

        except ZeroDivisionError:
            return 0.0

        return (change - overall_change) * 100

    def get_metric_change_direction(self, relative_change: float, target_metric_direction: str):
        """
        Method to indentify the direction of metric change.
        Params:
            relative_change: relative change we calculated in calculate_segment_relative_change.
            target_metric_direction: It could be "increasing" or "decreasing".
        """
        if (relative_change > 0 and target_metric_direction == TargetAim.MAXIMIZE) or (
            relative_change < 0 and target_metric_direction == TargetAim.MINIMIZE
        ):
            return MetricChangeDirection.UPWARD
        elif (relative_change > 0 and target_metric_direction == TargetAim.MINIMIZE) or (
            relative_change < 0 and target_metric_direction == TargetAim.MAXIMIZE
        ):
            return MetricChangeDirection.DOWNWARD
        elif relative_change == 0.0:
            return MetricChangeDirection.UNCHANGED

    def write_dataframe_to_csv(self, df: pd.DataFrame):
        """
        Using this function to write the provided data frame to a CSV, which would be used
        Dsensei server request
        Args: dataFrame of the data on which processing would be done
        Response: file_path, where the file is uploaded
        """
        file_path = os.path.join(self.PATH_FOR_CSV, f"{uuid.uuid4()}.csv")
        df.to_csv(file_path)
        return file_path

    async def send_file_to_dsensei(self, file_path: str):
        """
        To send the data csv file to dsensei server,
        this data would be used for calculation of insights on desensei
        server
        Args:
            file_path: Path of the CSV file containing the data
            on which the calculation needs to be done
        Response:
            file_id: ID of the file on dsensei server,
            dsensei server will use this ID to identify
            the file on which it would do the calculations for segment drift.
        """
        with open(file_path, "rb") as file:
            response = await self.http_request(url="api/v1/source/file/schema", files={"file": file})
            return response.get("name")

    async def http_request(self, url: str, *args: list, **kwargs: dict) -> dict:
        """
        Simple function for sending requests to dsensei server.
        Params:
            url: api endpoint of the dsensei server.
            args: we are not using for now
            Kwargs: can have date or files keys or both.
        """
        async with httpx.AsyncClient() as client:
            response = await client.post(
                urllib.parse.urljoin(str(self.DSENSEI_BASE_URL), url),
                json=kwargs.get("data"),
                files=kwargs.get("files"),
            )

            return response.json()

    async def get_insights(
        self,
        csv_file_id: str,
        evaluation_start_date: str,
        evaluation_end_date: str,
        comparison_start_date: str,
        comparison_end_date: str,
        dimensions: list[str],
        metric_id: str = "value",
        date_column: str = "date",
        aggregation_option: str = "sum",
        aggregation_method: str = "SUM",
    ) -> dict:

        # The name of the fields in request body of dsensei insight api request are different,
        # from what we have in segement drift request,
        # like evaluation_start_date -> baseDateRange
        # Few additional mandatory but nullable fields are also required in request body
        """
        Request payload structure:
        {
            "csv_file_id": "file_id we received in response from dsensei server",
            "baseDateRange": {"from": "request's evaluation_start_date", "to": "request's evaluation_end_date"},
            "comparisonDateRange": {"from": "request's comparison_start_date", "to": "request's comparison_end_date"},
            "dateColumn": "date field of our data",
            "metricColumn": {"aggregationOption": "It could be sum, nunique, count, ratio",
                             "singularMetric": {
                                 "columnName": "request's metric column on which we want to calculate segment drift",
                                 }
                            },
            "groupByColumns": [list of dimensions],
            "expectedValue": 0,  # we can ignore its value, but we have to keep it in request payload
            "filters": [],  # we can ignore its value, but we have to keep it in request payload
        }
        """
        request_payload = self.get_insight_request_payload(
            csv_file_id,
            evaluation_start_date,
            evaluation_end_date,
            comparison_start_date,
            comparison_end_date,
            dimensions,
            metric_id,
            date_column,
            aggregation_option,
            aggregation_method,
        )

        return await self.http_request(url="api/v1/insight/file/metric", data=request_payload)

    def get_insight_request_payload(
        self,
        csv_file_id: str,
        evaluation_start_date: str,
        evaluation_end_date: str,
        comparison_start_date: str,
        comparison_end_date: str,
        dimensions: list[str],
        metric_id: str = "value",
        date_column: str = "date",
        aggregation_option: str = "sum",
        aggregation_method: str = "SUM",
    ) -> dict:
        """
        Keys in our request are not matching with insight request payload so,
        Creating the payload with keys supported by dsensei server for insight request,
        args:
            data : dict consist of all the request attributes for segment drift like evaluation start date, end date etc
            csv_file_id: ID of the csv file uploaded to dsensei server.
        response:
            compatible payload dict for dsensei server insight request
        """
        request_payload: dict[str, Any] = dict()

        # Evaluation Start Date
        request_payload["baseDateRange"] = dict()
        request_payload["baseDateRange"]["from"] = self.update_dateformat(evaluation_start_date)

        # Evaluation End Date
        request_payload["baseDateRange"]["to"] = self.update_dateformat(evaluation_end_date)

        # Comparison Start Date
        request_payload["comparisonDateRange"] = dict()
        request_payload["comparisonDateRange"]["from"] = self.update_dateformat(comparison_start_date)

        # Comparison End Date
        request_payload["comparisonDateRange"]["to"] = self.update_dateformat(comparison_end_date)

        # file_id of the generated CSV, change it later
        request_payload["fileId"] = csv_file_id

        # Metric for Calculation
        """
            "metricColumn": {
                "aggregationOption": "",      any one option from these ["sum", "count", "nunique"]
                "singularMetric": {
                  "columnName": "metricColumn",
                  "aggregationMethod": "",
                }
            },
        """
        request_payload["metricColumn"] = {
            "aggregationOption": aggregation_option,
            "singularMetric": {"columnName": metric_id},
        }
        request_payload["aggregationMethod"] = aggregation_method

        # Dimensions
        request_payload["groupByColumns"] = dimensions
        request_payload["dateColumn"] = date_column
        # Mandatory keys required for request
        request_payload["expectedValue"] = 0
        request_payload["filters"] = []
        return request_payload

    def update_dateformat(self, date_obj):
        return datetime.strptime(date_obj, "%Y-%m-%d").strftime(self.TIMESTAMP_FORMAT)

    def validate_request_data(self, df: pd.DataFrame, *args, **kwargs):
        """
        Method to validate the data we received for segement drift calculation
        Params:
            df: Dataframe of the data on which the calculation will be performed.
            args: we are not using for now
            kwargs: for now we are using the keyword args aggregationOption, aggregationMethod, and metric_column.
        """
        agg_options_mapping = {"sum": "SUM", "count": "COUNT", "nunique": "DISTINCT"}

        # invalid aggregationOption
        if kwargs["aggregation_option"] not in agg_options_mapping:
            raise InvalidRequestError("Aggregation option must be one of ['sum', 'count', 'nunique']")

        # invalid aggregationMethod
        if kwargs["aggregation_method"] != agg_options_mapping[kwargs["aggregation_option"]]:
            raise InvalidRequestError(
                f"Aggregation method should be {agg_options_mapping[kwargs['aggregation_option']]},"
                f"for Aggregation Option {kwargs['aggregation_option']}"
            )

        # invalid metric column
        if kwargs.get("metric_column") not in df:
            raise InvalidRequestError("Provided metric column name must exist in data")
