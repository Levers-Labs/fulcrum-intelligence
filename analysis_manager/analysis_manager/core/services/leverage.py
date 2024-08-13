import logging

import pandas as pd

from analysis_manager.core.models.leverage_id import Leverage, LeverageDetails
from commons.clients.query_manager import QueryManagerClient
from fulcrum_core import AnalysisManager, modules

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LeverageIdService:
    def __init__(self, analysis_manager: AnalysisManager, query_manager: QueryManagerClient):
        self.analysis_manager = analysis_manager
        self.query_manager = query_manager

    async def calculate_leverage_id(
        self,
        metric: dict,
        values_df: pd.DataFrame,
        max_values: dict,
    ) -> Leverage:
        """
        Calculate leverage id for a given metric.

        Args:
            :param metric:
            :param max_values:
            :param values_df:

        Returns:
            Leverage object containing the leverage results.

        """

        # Initialize the LeverageCalculator with the metric data
        calculator = modules.LeverageCalculator()

        # Run the calculator to get the results
        final_result = calculator.run(metric, values_df, max_values)

        # Convert the final result to the Leverage model
        def convert_to_leverage(component):
            details = LeverageDetails(
                pct_diff=component["parent_percentage_difference"] if component["parent_percentage_difference"] else 0,
                pct_diff_root=(
                    component["top_parent_percentage_difference"]
                    if component["top_parent_percentage_difference"]
                    else 0
                ),
            )
            return Leverage(
                metric_id=component["metric_id"],
                leverage=details,
                components=(
                    [convert_to_leverage(c) for c in component["components"]] if component["components"] else None
                ),
            )

        leverage_details = LeverageDetails(
            pct_diff=(
                final_result["parent_percentage_difference"] if final_result["parent_percentage_difference"] else 0
            ),
            pct_diff_root=(
                final_result["top_parent_percentage_difference"]
                if final_result["top_parent_percentage_difference"]
                else 0
            ),
        )

        leverage = Leverage(
            metric_id=final_result["metric_id"],
            leverage=leverage_details,
            components=(
                [convert_to_leverage(c) for c in final_result["components"]] if final_result["components"] else None
            ),
        )
        return leverage
