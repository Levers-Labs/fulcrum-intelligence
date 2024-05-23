import os
import re
import tempfile
import urllib
import uuid
from datetime import date, datetime
from typing import Any

import httpx
import pandas as pd
from sqlalchemy.exc import InvalidRequestError

from fulcrum_core.enums import (
    AggregationMethod,
    AggregationOption,
    MetricAim,
    MetricChangeDirection,
)


class SegmentDriftEvaluator:
    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

    def __init__(self, dsensei_base_url: str):
        self.dsensei_base_url = dsensei_base_url
        self.temp_dir = tempfile.gettempdir()

    async def calculate_segment_drift(
        self,
        df: pd.DataFrame,
        evaluation_start_date: date,
        evaluation_end_date: date,
        comparison_start_date: date,
        comparison_end_date: date,
        dimensions: list[str],
        metric_column: str = "value",
        date_column: str = "date",
        aggregation_option: AggregationOption = AggregationOption.SUM,
        aggregation_method: AggregationMethod = AggregationMethod.SUM,
        target_metric_direction: MetricAim = MetricAim.INCREASING,
    ) -> dict[str, Any]:
        """
        This is the entrypoint for calculating the segment drift in the given data.
        Steps:
        1. we are converting the dataframe to csv file as dsensei server uses csv files as its data source.
        2. Converted CSV file will be sent to dsensei server, in response we will get the ID of the file.
        3. We will send the insight request to dsensei server to calculate the segment drift.

        """
        self.validate_request_data(
            df,
            metric_column=metric_column,
        )

        csv_path = self.write_dataframe_to_csv(df)

        # Uploading the file to dsensei server, as it works with CSV files only.
        file_id = await self.send_file_to_dsensei(csv_path)

        # removing the generated file
        if os.path.exists(csv_path):
            os.remove(csv_path)

        # fetching the insights of our data from dsensei server, get_insights
        result = await self.get_insights(
            csv_file_id=file_id,
            evaluation_start_date=evaluation_start_date,
            evaluation_end_date=evaluation_end_date,
            comparison_start_date=comparison_start_date,
            comparison_end_date=comparison_end_date,
            dimensions=dimensions,
            metric_column=metric_column,
            date_column=date_column,
            aggregation_option=aggregation_option,
            aggregation_method=aggregation_method,
        )
        # dimension slice info consist of all calculations for all permutations of the provided dimensions
        dimension_slices = result["dimension_slices"]
        overall_change = self.get_overall_change(
            comparison_value=result["comparison_value"],
            baseline_value=result["evaluation_value"],
        )

        # processing the result to calculate the direction of metric change, whether its UPWARD DOWNWARD or UNCHANGED
        for segment in dimension_slices:
            relative_change = self.calculate_segment_relative_change(segment, overall_change=overall_change)

            segment["relative_change"] = relative_change

            segment["pressure"] = self.get_metric_change_direction(
                relative_change=relative_change,
                target_metric_direction=target_metric_direction,
            )

        return result

    def get_overall_change(self, comparison_value: float, baseline_value: float) -> float:
        try:
            overall_change = (comparison_value - baseline_value) / baseline_value
        except ZeroDivisionError:
            overall_change = 0.0
        return overall_change

    def calculate_segment_relative_change(self, segment: dict, overall_change: float) -> float:
        try:
            slice_comparison_value = segment["comparison_value"]["slice_value"]
        except KeyError:
            slice_comparison_value = 0

        try:
            slice_evaluation_value = segment["evaluation_value"]["slice_value"]
        except KeyError:
            slice_evaluation_value = 0

        try:
            change = (slice_comparison_value - slice_evaluation_value) / slice_evaluation_value

        except ZeroDivisionError:
            return 0.0

        return (change - overall_change) * 100

    def get_metric_change_direction(
        self, relative_change: float, target_metric_direction: str
    ) -> MetricChangeDirection:
        """
        Method to indentify the direction of metric change.
        Params:
            relative_change: relative change we calculated in calculate_segment_relative_change.
            target_metric_direction: It could be "increasing" or "decreasing".
        """
        if (relative_change > 0 and target_metric_direction == MetricAim.INCREASING) or (
            relative_change < 0 and target_metric_direction == MetricAim.DECREASING
        ):
            return MetricChangeDirection.UPWARD
        elif (relative_change > 0 and target_metric_direction == MetricAim.DECREASING) or (
            relative_change < 0 and target_metric_direction == MetricAim.INCREASING
        ):
            return MetricChangeDirection.DOWNWARD

        return MetricChangeDirection.UNCHANGED

    def write_dataframe_to_csv(self, df: pd.DataFrame) -> str:
        """
        Using this function to write the provided data frame to a CSV, which would be used
        Dsensei server request
        Args: dataFrame of the data on which processing would be done
        Response: file_path, where the file is uploaded
        """
        file_path = os.path.join(self.temp_dir, f"{uuid.uuid4()}.csv")
        df.to_csv(file_path)
        return file_path

    async def send_file_to_dsensei(self, file_path: str) -> str:
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
            return response["name"]

    async def http_request(self, url: str, *args: list, **kwargs: dict) -> dict:
        """
        Simple function for sending requests to dsensei server.
        Params:
            url: api endpoint of the dsensei server.
            args: we are not using for now
            Kwargs: can have json or files keys or both.
        """
        async with httpx.AsyncClient() as client:
            response = await client.post(
                urllib.parse.urljoin(
                    str(self.dsensei_base_url),
                    url,
                ),
                **kwargs,  # type: ignore
            )

            return response.json()

    async def get_insights(
        self,
        csv_file_id: str,
        evaluation_start_date: date,
        evaluation_end_date: date,
        comparison_start_date: date,
        comparison_end_date: date,
        dimensions: list[str],
        metric_column: str = "value",
        date_column: str = "date",
        aggregation_option: AggregationOption = AggregationOption.SUM,
        aggregation_method: AggregationMethod = AggregationMethod.SUM,
    ) -> dict:
        """
        - The name of the fields in request body of dsensei insight api request are different,
          from what we have in segement drift request,
          like evaluation_start_date -> baseDateRange
        - Few additional mandatory but nullable fields are also required in request body

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
            csv_file_id=csv_file_id,
            evaluation_start_date=evaluation_start_date,
            evaluation_end_date=evaluation_end_date,
            comparison_start_date=comparison_start_date,
            comparison_end_date=comparison_end_date,
            dimensions=dimensions,
            metric_column=metric_column,
            date_column=date_column,
            aggregation_option=aggregation_option,
            aggregation_method=aggregation_method,
        )

        # main_key is the parent key of the whole result, its format is "metricColumn_aggregationMethod"
        # EX: "value_SUM"  or "region_DISTINCT"
        main_key = f"{metric_column}_{aggregation_method}"
        result = await self.http_request(url="api/v1/insight/file/metric", json=request_payload)
        return self.post_process_insight_response(result[main_key])

    def get_insight_request_payload(
        self,
        csv_file_id: str,
        evaluation_start_date: date,
        evaluation_end_date: date,
        comparison_start_date: date,
        comparison_end_date: date,
        dimensions: list[str],
        metric_column: str = "value",
        date_column: str = "date",
        aggregation_option: AggregationOption = AggregationOption.SUM,
        aggregation_method: AggregationMethod = AggregationMethod.SUM,
    ) -> dict:
        """
        Keys in our request are not matching with insight request payload so,
        Creating the payload with keys supported by dsensei server for insight request,
        args:
            data : dict consist of all the request attributes for segment drift like evaluation start date, end date etc
            csv_file_id: ID of the csv file uploaded to dsensei server.
        response:
            compatible payload dict for dsensei server insight request

        Metric Column for the request is a nested structure as shown below:
            "metricColumn": {
                "aggregationOption": "",      any one option from these ["sum", "count", "nunique"]
                "singularMetric": {
                  "columnName": "metricColumn",
                  "aggregationMethod": "",
                }
            },

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
        request_payload["metricColumn"] = {
            "aggregationOption": aggregation_option,
            "singularMetric": {"columnName": metric_column},
        }
        request_payload["aggregationMethod"] = aggregation_method

        # Dimensions
        request_payload["groupByColumns"] = dimensions
        request_payload["dateColumn"] = date_column
        # Mandatory keys required for request
        request_payload["expectedValue"] = 0
        request_payload["filters"] = []
        return request_payload

    def update_dateformat(self, date_obj: date) -> str:
        # Convert date object to datetime object (assuming midnight as the time)
        datetime_object = datetime.combine(date_obj, datetime.min.time())

        # Convert datetime object to datetime string
        return datetime_object.strftime(self.TIMESTAMP_FORMAT)  # Example format: YYYY-MM-DD HH:MM:SS

    def validate_request_data(self, df: pd.DataFrame, *args, **kwargs):
        """
        Method to validate the data we received for segement drift calculation
        Params:
            df: Dataframe of the data on which the calculation will be performed.
            args: we are not using for now
            kwargs: for now we are using the keyword args metric_column, in future we can add validation for other
                    columns as well.
        """

        # invalid metric column
        if kwargs.get("metric_column") not in df:
            raise InvalidRequestError("Provided metric column name must exist in data")

    def post_process_insight_response(self, response: dict[str, Any]) -> dict[str, Any]:
        """
        In this function we are working with dict, key value pairs,
        Goal here is to convert camelcase keys to snake case
        - convert few values which are stored as dict to a list
        Ex: dimensions are stored as a key value pair in insight response,
            converting it to list as key also exist in value.
            From => {region: {"name": "region", "score": 0.6888888888888889, "is_key_dimension": True}, ...}
            to this => [{"name": "region", "score": 0.6888888888888889, "is_key_dimension": True}, ...]

        - Same conversion for dimensions slices info
        """
        processed_response = self.convert_keys_camel_to_snake_case(dict(), response)

        processed_response["dimensions"] = list(processed_response["dimensions"].values())
        processed_response["dimension_slices"] = list(processed_response["dimension_slice_info"].values())
        processed_response["dimension_slices_permutation_Keys"] = processed_response["top_driver_slice_keys"]
        del processed_response["top_driver_slice_keys"]
        del processed_response["dimension_slice_info"]
        return processed_response

    def convert_keys_camel_to_snake_case(self, processed_response: dict, insight_response: dict) -> dict:
        """
        In this function we are recursively converting
        - camel case keys to snake case
        - replacing "baseline" with "evaluation" in keys
        Using recursion to perform above 2 operations in all the nested levels of dict
        """
        for key, value in insight_response.items():
            # converting the key, Camelcase to snake case here
            snake_case_key = re.sub(r"(?<!^)(?=[A-Z])", "_", key).lower()

            # replacing baseline with evaluation in key
            if snake_case_key.startswith("baseline"):
                snake_case_key = "evaluation" + snake_case_key[len("baseline") :]

            # if the value is of type dict we will call the function recursively to convert the nested dict keys as well
            if isinstance(value, dict):
                processed_response[snake_case_key] = self.convert_keys_camel_to_snake_case(dict(), value)

            elif isinstance(value, list):
                if snake_case_key not in processed_response:
                    processed_response[snake_case_key] = []

                for item in value:
                    # converting nested dict keys with recursion
                    if isinstance(item, dict):
                        processed_response[snake_case_key].append(self.convert_keys_camel_to_snake_case(dict(), item))
                    else:
                        processed_response[snake_case_key].append(item)
            else:
                processed_response[snake_case_key] = value

        return processed_response
