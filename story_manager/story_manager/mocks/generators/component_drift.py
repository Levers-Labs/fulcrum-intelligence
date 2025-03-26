import random
from datetime import date
from typing import Any

from commons.models.enums import Granularity
from story_manager.core.enums import (
    Pressure,
    StoryGenre,
    StoryGroup,
    StoryType,
)
from story_manager.mocks.generators.base import MockGeneratorBase


class ComponentDriftMockGenerator(MockGeneratorBase):
    """Mock generator for Component Drift stories"""

    genre = StoryGenre.ROOT_CAUSES
    group = StoryGroup.COMPONENT_DRIFT
    min_component_count = 2

    def generate_stories(
        self, metric: dict[str, Any], grain: Granularity, story_date: date | None = None
    ) -> list[dict[str, Any]]:
        """Generate mock component drift stories"""
        if story_date:
            self.data_service.story_date = story_date

        stories = []

        # Extract components from metric expression or use direct components if available
        components = self._extract_components_from_metric(metric)

        # Check if we have enough components
        if len(components) < self.min_component_count:
            return []

        # Generate mock component data
        component_data = self._generate_mock_component_data(components)

        # Create ranked DataFrame of components
        ranked_components = self._create_ranked_df(component_data)

        # Get top 4 components
        top_components = ranked_components[: min(4, len(ranked_components))]

        # Generate stories for top components
        for component in top_components:
            # Generate time series for this component
            time_series = self.get_mock_component_series(component)

            # Generate variables for this component
            variables = self.get_mock_variables(metric, component["story_type"], grain, time_series, component)

            # Create story
            story = self.prepare_story_dict(
                metric,
                component["story_type"],
                grain,
                time_series,
                variables,
                story_date or self.data_service.story_date,
            )

            stories.append(story)

        return stories

    def _extract_components_from_metric(self, metric: dict[str, Any]) -> list[str]:
        """Extract component metric IDs from the metric expression or direct components."""
        components = []

        # First check if components are directly available
        if metric.get("components") and len(metric.get("components", [])) >= self.min_component_count:
            return [comp for comp in metric.get("components", [])]

        # If not, try to extract from metric_expression
        expression = metric.get("metric_expression", {})

        # Extract from expression if it exists
        if expression and expression.get("expression"):
            # Get the operands from the expression
            operands = expression.get("expression", {}).get("operands", [])
            for operand in operands:
                if operand.get("type") == "metric" and operand.get("metric_id"):
                    components.append(operand.get("metric_id"))

        return components

    @staticmethod
    def get_mock_component_series(component: dict[str, Any]) -> list[dict[str, Any]]:
        return [
            {
                "component_id": component["metric_id"],
                "evaluation_value": component["evaluation_value"],
                "comparison_value": component["comparison_value"],
                "percentage_drift": component["percentage_drift"] / 100,
            }
        ]

    def get_mock_time_series(self, grain: Granularity, story_type: StoryType) -> list[dict[str, Any]]:
        """Required by base class but not used for component drift"""
        return []

    def get_mock_variables(
        self,
        metric: dict[str, Any],
        story_type: StoryType,
        grain: Granularity,
        time_series: list[dict[str, Any]] | None = None,
        component: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Generate mock variables for component drift stories"""
        # Create variables dict
        return {
            "metric": {"id": metric["id"], "label": metric["label"]},
            "grain": grain.value,
            "component": component["metric_id"],  # type: ignore
            "percentage_drift": round(abs(component["percentage_drift"]), 2),  # type: ignore
            "relative_impact": round(abs(component["relative_impact"]), 2),  # type: ignore
            "contribution": round(abs(component["marginal_contribution_root"]), 2),  # type: ignore
            "pressure": component["pressure"],  # type: ignore
            "evaluation_value": component["evaluation_value"],  # type: ignore
            "comparison_value": component["comparison_value"],  # type: ignore
        }

    def _generate_mock_component_data(self, components: list[str]) -> list[dict[str, Any]]:
        """Generate mock data for components"""
        component_data = []

        # Total contribution should sum to 100%
        total_contribution = 100.0
        remaining_contribution = total_contribution

        # Make sure we have at least one improving and one worsening component
        improving_count = max(1, len(components) // 2)

        for i, component_id in enumerate(components):
            # For the last component, use the remaining contribution
            if i == len(components) - 1:
                contribution = remaining_contribution
            else:
                # Randomly distribute contribution, ensuring some is left for remaining components
                max_contribution = remaining_contribution - (len(components) - i - 1) * 5
                contribution = random.uniform(5, max(10, max_contribution))  # noqa
                remaining_contribution -= contribution

            # Decide if component is improving or worsening
            is_improving = i < improving_count

            # Generate evaluation and comparison values with significant differences
            base_value = random.uniform(500, 2000)  # noqa

            if is_improving:
                # For improving component, evaluation > comparison
                comparison_value = base_value
                evaluation_value = base_value * random.uniform(1.2, 1.5)  # noqa
                story_type = StoryType.IMPROVING_COMPONENT
                pressure = Pressure.UPWARD.value
                # Calculate percentage drift for improving component
                percentage_drift = ((evaluation_value - comparison_value) / comparison_value) * 100
                # Make contribution positive
                contribution = abs(contribution)
            else:
                # For worsening component, evaluation < comparison
                comparison_value = base_value
                evaluation_value = base_value * random.uniform(0.5, 0.8)  # noqa
                story_type = StoryType.WORSENING_COMPONENT
                pressure = Pressure.DOWNWARD.value
                # Calculate percentage drift for worsening component
                percentage_drift = ((comparison_value - evaluation_value) / comparison_value) * 100
                # Make contribution negative for worsening components
                contribution = -abs(contribution)

            # Calculate relative impact (random portion of contribution)
            relative_impact = abs(contribution) * random.uniform(0.7, 1.0)  # noqa

            component_data.append(
                {
                    "metric_id": component_id,
                    "evaluation_value": round(evaluation_value),
                    "comparison_value": round(comparison_value),
                    "marginal_contribution_root": contribution,
                    "relative_impact": relative_impact,
                    "percentage_drift": percentage_drift,
                    "story_type": story_type,
                    "pressure": pressure,
                }
            )

        return component_data

    def _create_ranked_df(self, component_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Rank components by absolute marginal contribution"""
        # Sort by absolute marginal contribution in descending order
        return sorted(component_data, key=lambda x: abs(x["marginal_contribution_root"]), reverse=True)
