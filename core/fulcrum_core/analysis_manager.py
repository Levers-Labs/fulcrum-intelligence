import logging
from datetime import date
from typing import Any

import pandas as pd

from fulcrum_core.correlate import correlate
from fulcrum_core.describe import describe
from fulcrum_core.enums import Granularity
from fulcrum_core.modules import ComponentDriftEvaluator, ProcessControlAnalyzer, SimpleForecast

logger = logging.getLogger(__name__)


class AnalysisManager:
    """
    Core class for implementing all major functions for analysis manager
    """

    def cal_average_growth(self, series: pd.Series, precision: int | None = None) -> float:
        """
        Calculate the average growth rate for the given time series data.
        The average growth rate is mean of the growth rate (pct_change) of the time series data.
        Avoid inf average growth, if present in the data.

        :param series: The input time series data.
        :param precision: The number of decimal places to round the result.
        :return: The average growth rate for the time series data.
        """
        growth_rate = series.pct_change()
        avg_growth = growth_rate.mean()
        # Avoid inf average growth
        if avg_growth == float("inf"):
            avg_growth = growth_rate[growth_rate != float("inf")].mean()
        # convert to percentage
        avg_growth = avg_growth * 100
        return round(avg_growth, precision) if precision else round(avg_growth)

    def describe(
        self,
        data: list[dict],
        dimensions: list[str],
        metric_id: str,
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
        aggregation_function: str,
    ) -> list[dict]:
        result = describe(data, dimensions, metric_id, start_date, end_date, aggregation_function)
        return result

    def correlate(self, data: pd.DataFrame, start_date: date, end_date: date) -> list[dict]:
        result = correlate(data, start_date, end_date)
        return result

    @staticmethod
    def process_control(df: pd.DataFrame) -> pd.DataFrame:
        """
        Perform time series analysis using process control techniques.
        For a given metric time series, Process Control generates:
            Half-averages based on the first 10-18 Individual Values.
            Central Line values for each time period, using those half-averages
            Upper and Lower Control Limits for each time period based on the Central Line.
            Slope of the Central Line.
            Signal detection based on the Central Line and Control Limits.

        Args:
            df (pd.DataFrame): The input time series data with 'date' and 'value' columns.

        Returns:
            pd.DataFrame: The analyzed time series data with additional calculated columns.
            The result DataFrame will have the following columns:
                - 'date': The date of the time period.
                - 'value': The value of the metric for the time period.
                - 'central_line': The central line value for the time period.
                - 'ucl': The upper control limit value for the time period.
                - 'lcl': The lower control limit value for the time period.
                - 'slope': The slope of the central line for the time period.
                - 'slope_change': The change in slope from the previous time period.
                - 'trend_signal_detected': The signal detection result for the time period.
        """
        analyzer = ProcessControlAnalyzer()
        res_df = analyzer.analyze(df)
        return res_df

    @staticmethod
    def calculate_component_drift(
        metric_id: str,
        values: list[dict[str, Any]],
        metric_expression: dict[str, Any],
        parent_drift: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Calculate the drift for the given expression.
        e.g CAC = (SalesDevSpend + MktSpend) / (NewCust - LostCust)
        expression = {
            "type": "expression",
            "operator": "/",
            "operands": [
                {
                    "type": "expression",
                    "operator": "+",
                    "operands": [
                        {"type": "metric", "metric_id": "SalesDevSpend"},
                        {"type": "metric", "metric_id": "MktSpend"}
                    ],
                },
                {
                    "type": "expression",
                    "operator": "-",
                    "operands": [
                        {"type": "metric", "metric_id": "NewCust"},
                        {"type": "metric", "metric_id": "LostCust"}
                    ],
                }
            ],
        }
        :return: drift information for the expression
        e.g. {
            "metric_id": "CAC",
            "evaluation_value": 100.0,
            "comparison_value": 90.0,
            "drift": {
                "absolute_drift": 10.0,
                "percentage_drift": 11.11,
                "relative_impact": 100,
                "marginal_contribution": 100,
                "relative_impact_root": 100,
                "marginal_contribution_root": 100
            },
            "components": [
                {
                    "metric_id": "SalesDevSpend",
                    "evaluation_value": 30.0,
                    "comparison_value": 25.0,
                    "drift": {
                        "absolute_drift": 5.0,
                        "percentage_drift": 20.0,
                        "relative_impact": 50.0,
                        "marginal_contribution": 10.0,
                        "relative_impact_root": 50.0,
                        "marginal_contribution_root": 10.0
                    }
                },
                {
                    "metric_id": "MktSpend",
                    "evaluation_value": 35.0,
                    "comparison_value": 35.0,
                    "drift": {
                        "absolute_drift": 4.0,
                        "percentage_drift": 12.9,
                        "relative_impact": 40.0,
                        "marginal_contribution": 5.2,
                        "relative_impact_root": 40.0,
                        "marginal_contribution_root": 5.2
                    }
                },
                {
                    "metric_id": "NewCust",
                    "evaluation_value": 20.0,
                    "comparison_value": 22.0,
                    "drift": {
                        "absolute_drift": -2.0,
                        "percentage_drift": -9.1,
                        "relative_impact": -20.0,
                        "marginal_contribution": -2.0,
                        "relative_impact_root": -20.0,
                        "marginal_contribution_root": -2.0
                    }
                },
                {
                    "metric_id": "LostCust",
                    "evaluation_value": 11.0,
                    "comparison_value": 9.0,
                    "drift": {
                        "absolute_drift": 2.0,
                        "percentage_drift": 22.2,
                        "relative_impact": 20.0,
                        "marginal_contribution": 4.0,
                        "relative_impact_root": 20.0,
                        "marginal_contribution_root": 4.0
                    }
                }
            ]
        }
        """
        evaluator = ComponentDriftEvaluator(values)
        expression = evaluator.resolve_expression_values(metric_expression)
        # Get the relative impact and marginal contribution of the root node
        relative_impact_root = 1
        marginal_contribution_root = 1
        if parent_drift:
            relative_impact_root = parent_drift["relative_impact_root"]
            marginal_contribution_root = parent_drift["marginal_contribution_root"]
        result = evaluator.calculate_drift(
            expression, relative_impact_root=relative_impact_root, marginal_contribution_root=marginal_contribution_root
        )
        # Return the drift information as expected by the API
        response = {
            "metric_id": metric_id,
            "evaluation_value": result["evaluation_value"],
            "comparison_value": result["comparison_value"],
            "drift": result.get("drift"),
            "components": [],
        }

        def add_metric_components(node):
            if node.get("type") == "metric":
                response["components"].append(
                    {
                        "metric_id": node.get("metric_id"),
                        "evaluation_value": node.get("evaluation_value"),
                        "comparison_value": node.get("comparison_value"),
                        "drift": node.get("drift"),
                    }
                )
            elif "operands" in node:
                for operand in node["operands"]:
                    add_metric_components(operand)

        add_metric_components(result)
        logger.debug(
            "Component drift calculated for metric: %s, values: %s, expression: %s & \n drift: %s",
            metric_id,
            values,
            metric_expression,
            response,
        )
        return response

    def simple_forecast(
        self,
        df: pd.DataFrame,
        grain: Granularity,
        forecast_horizon: int | None = None,
        forecast_till_date: date | None = None,
        conf_interval: float | None = None,
    ) -> list[dict]:
        """
        perform simple forecasting on the given time series data.
        the model is trained on the input data and used to forecast the future values.
        for the period of the latest data point to the end date, n predictions are made
        where n is the number of periods between the latest data point and the end date each granular level.

        Args:
            df (pd.DataFrame): The input time series data with 'date' and 'value' columns.
            grain (str): The granularity of the data. supported values are 'day', 'week', 'month', and 'quarter'.
            forecast_horizon (int): The number of periods to forecast into the future.
            forecast_till_date (pd.Timestamp): The end date for the forecast.
            conf_interval (float): The confidence interval for the forecast. default is 0.95.

        Returns:
            list[dict]: The forecasted time series data with additional calculated columns.
            the result list will have the following dictionary for each time period:
                - 'date': The date of the time period.
                - 'value': The value of the metric for the time period.
                - 'confidence_interval': The confidence intervals for the forecasted value.
        """
        if forecast_horizon is None and forecast_till_date is None:
            raise ValueError("Either forecast_horizon or forecast_till_date should be provided")

        kwargs = {}
        if conf_interval:
            kwargs["conf_interval"] = conf_interval
        forecast = SimpleForecast(df, grain=grain)
        results = []
        if forecast_horizon:
            results = forecast.predict_n(forecast_horizon, **kwargs)
        elif forecast_till_date:
            results = forecast.predict_till_date(forecast_till_date, **kwargs)
        return results

    @staticmethod
    def calculate_growth_rates_of_series(series_df: pd.DataFrame) -> pd.Series:
        """
        Calculate the growth rates for each data point in the time series.

        Parameters:
        - series_df (pd.DataFrame): The time series data frame containing the values.

        Returns:
        pd.Series: Series of growth rates.
        """

        growth_rates = series_df["value"].pct_change() * 100

        return growth_rates

    @staticmethod
    def calculate_deviation(value: pd.Series, limit: pd.Series) -> pd.Series:
        """
        Calculate the deviation percentage based on an observed value and a reference limit.

        :param value: The observed value for which deviation is calculated.
        :param limit: The upper or lower limit.

        :return: The deviation percentage as a Pandas Series containing a single value.
        """

        # Check if all values in the 'limit' Series are zero
        if (limit == 0).all():
            return pd.Series([0.0])

        deviation = round(((value - limit) / limit) * 100)
        return pd.Series(deviation)
