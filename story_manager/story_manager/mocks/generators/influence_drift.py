import random
from datetime import date
from typing import Any, Dict, List

from commons.models.enums import Granularity
from story_manager.core.enums import (
    Movement,
    Pressure,
    StoryGenre,
    StoryGroup,
    StoryType,
)
from story_manager.mocks.generators.base import MockGeneratorBase
from story_manager.mocks.services.data_service import MockDataService
from story_manager.story_builder.constants import GRAIN_META, STORY_GROUP_TIME_DURATIONS


class InfluenceDriftMockGenerator(MockGeneratorBase):
    """Mock generator for Influence Drift stories"""

    genre = StoryGenre.ROOT_CAUSES
    group = StoryGroup.INFLUENCE_DRIFT
    supported_grains = [Granularity.DAY, Granularity.WEEK, Granularity.MONTH]

    def __init__(self, mock_data_service: MockDataService):
        self.data_service = mock_data_service

    def generate_stories(
        self, metric: dict[str, Any], grain: Granularity, story_date: date = None
    ) -> list[dict[str, Any]]:
        """Generate mock influence drift stories"""
        if story_date:
            self.data_service.story_date = story_date

        stories = []

        # Check if metric has influencers
        if not metric.get("influencers"):
            return []

        # Get influencers from metric
        influencers = metric.get("influencers", [])

        # Generate mock influence data
        influence_data = self._generate_mock_influence_data(influencers, metric)

        # Generate stories for each influence
        for influence in influence_data:
            # Generate relationship strength stories (Stronger/Weaker Influence)
            relationship_series = self.get_mock_time_series(grain, influence["relationship_story_type"], influence)
            relationship_vars = self.get_mock_variables(
                metric, influence["relationship_story_type"], grain, relationship_series, influence
            )
            relationship_story = self.prepare_story_dict(
                metric,
                influence["relationship_story_type"],
                grain,
                relationship_series,
                relationship_vars,
                story_date or self.data_service.story_date,
            )
            stories.append(relationship_story)

            # Generate influence metric stories (Improving/Worsening Influence)
            metric_series = self.get_mock_time_series(grain, influence["metric_story_type"], influence)
            metric_vars = self.get_mock_variables(
                metric, influence["metric_story_type"], grain, metric_series, influence
            )
            metric_story = self.prepare_story_dict(
                metric,
                influence["metric_story_type"],
                grain,
                metric_series,
                metric_vars,
                story_date or self.data_service.story_date,
            )
            stories.append(metric_story)

        return stories

    def get_mock_time_series(
        self, grain: Granularity, story_type: StoryType, influence: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Generate mock time series data for influence drift stories"""
        # Get date range
        start_date, end_date = self.data_service._get_input_time_range(grain, self.group)

        # Get dates within range
        dates = self.data_service.get_dates_for_range(grain, start_date, end_date)
        formatted_dates = self.data_service.get_formatted_dates(dates)

        # Generate values based on influence data
        values = []
        output_values = []

        # Get influence values
        latest_value = influence["latest_value"]
        previous_value = influence["previous_value"]

        # Get output values
        output_latest = influence["output_latest"]
        output_previous = influence["output_previous"]

        # Generate a series that shows the trend
        for i in range(len(formatted_dates)):
            # Calculate progress
            progress = i / (len(formatted_dates) - 1)

            # Interpolate between previous and latest values
            value = previous_value + progress * (latest_value - previous_value)
            output_value = output_previous + progress * (output_latest - output_previous)

            # Add some random noise
            noise = random.uniform(-0.05, 0.05) * value
            output_noise = random.uniform(-0.05, 0.05) * output_value

            value += noise
            output_value += output_noise

            values.append(round(value))
            output_values.append(round(output_value))

        # Create time series
        time_series = []
        for i, (date_str, value, output_value) in enumerate(zip(formatted_dates, values, output_values)):
            point = {
                "date": date_str,
                "value": value,
                "output_value": output_value,
                "influence_id": influence["influence_metric_id"],
            }
            time_series.append(point)

        return time_series

    def get_mock_variables(
        self,
        metric: dict[str, Any],
        story_type: StoryType,
        grain: Granularity,
        time_series: list[dict[str, Any]] = None,
        influence: dict[str, Any] = None,
    ) -> dict[str, Any]:
        """Generate mock variables for influence drift stories"""
        # Get grain metadata
        grain_meta = GRAIN_META[grain]

        # Create base variables dict
        variables = {
            "metric": {"id": metric["id"], "label": metric["label"]},
            "grain": grain.value,
            "eoi": grain_meta["eoi"],
            "pop": grain_meta["pop"],
            "interval": grain_meta["interval"],
            "influence_metric": influence["influence_metric_id"],
            "output_metric": metric["label"],
            "influence_deviation": round(abs(influence["influence_deviation"]), 2),
            "output_deviation": round(abs(influence["output_deviation"]), 2),
            "prev_output_deviation": round(abs(influence["prev_output_deviation"]), 2),
            "movement": influence["movement"],
            "pressure": influence["pressure"],
            "latest_strength": round(influence["latest_strength"], 2),
            "previous_strength": round(influence["previous_strength"], 2),
            "current_value": round(influence["output_latest"], 2),
        }

        return variables

    def _generate_mock_influence_data(self, influencers: list[str], metric: dict[str, Any]) -> list[dict[str, Any]]:
        """Generate mock data for influences"""
        influence_data = []

        # Generate output metric values
        output_latest = random.uniform(500, 2000)
        output_previous = output_latest * random.uniform(0.8, 1.2)
        output_prev_to_prev = output_previous * random.uniform(0.8, 1.2)

        # Calculate output deviations
        output_deviation = ((output_latest - output_previous) / output_previous) * 100
        prev_output_deviation = ((output_previous - output_prev_to_prev) / output_prev_to_prev) * 100

        # Determine movement and pressure
        movement = Movement.INCREASE.value if output_deviation > 0 else Movement.DECREASE.value
        pressure = Pressure.UPWARD.value if output_deviation > 0 else Pressure.DOWNWARD.value

        for influence_metric_id in influencers:
            # Generate influence values
            latest_value = random.uniform(500, 2000)
            previous_value = latest_value * random.uniform(0.8, 1.2)

            # Calculate influence deviation
            influence_deviation = ((latest_value - previous_value) / previous_value) * 100

            # Generate relationship strengths
            # Randomly decide if relationship is getting stronger or weaker
            is_stronger = random.choice([True, False])

            if is_stronger:
                # Relationship is getting stronger
                previous_strength = random.uniform(0.1, 0.5)
                latest_strength = previous_strength * random.uniform(1.2, 1.5)
                relationship_story_type = StoryType.STRONGER_INFLUENCE
            else:
                # Relationship is getting weaker
                previous_strength = random.uniform(0.5, 0.9)
                latest_strength = previous_strength * random.uniform(0.5, 0.8)
                relationship_story_type = StoryType.WEAKER_INFLUENCE

            # Calculate marginal contribution
            marginal_contribution = influence_deviation * latest_strength

            # Determine influence metric story type
            metric_story_type = (
                StoryType.IMPROVING_INFLUENCE if marginal_contribution > 0 else StoryType.WORSENING_INFLUENCE
            )

            influence_data.append(
                {
                    "influence_metric_id": influence_metric_id,
                    "latest_value": latest_value,
                    "previous_value": previous_value,
                    "output_latest": output_latest,
                    "output_previous": output_previous,
                    "influence_deviation": influence_deviation,
                    "output_deviation": output_deviation,
                    "prev_output_deviation": prev_output_deviation,
                    "movement": movement,
                    "pressure": pressure,
                    "latest_strength": latest_strength,
                    "previous_strength": previous_strength,
                    "marginal_contribution": marginal_contribution,
                    "relationship_story_type": relationship_story_type,
                    "metric_story_type": metric_story_type,
                }
            )

        return influence_data
