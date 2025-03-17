import random
from datetime import date
from typing import Any

from commons.models.enums import Granularity
from story_manager.core.enums import (
    Movement,
    Pressure,
    StoryGenre,
    StoryGroup,
    StoryType,
)
from story_manager.mocks.generators.base import MockGeneratorBase
from story_manager.story_builder.constants import GRAIN_META


class InfluenceDriftMockGenerator(MockGeneratorBase):
    """Mock generator for Influence Drift stories"""

    genre = StoryGenre.ROOT_CAUSES
    group = StoryGroup.INFLUENCE_DRIFT
    DEFAULT_MOCK_INFLUENCERS = ["Revenue", "Cost", "Customer Satisfaction", "Market Share", "Conversion Rate"]

    def generate_stories(
        self, metric: dict[str, Any], grain: Granularity, story_date: date | None = None
    ) -> list[dict[str, Any]]:
        """Generate mock influence drift stories"""
        if story_date:
            self.data_service.story_date = story_date

        # Get influencers from metric or create mock ones if none exist
        influencers = metric.get("influencers", [])
        if not influencers:
            # Create 2-3 mock influencers
            num_influencers = random.randint(2, 3)  # noqa
            influencers = self._create_mock_influencers(num_influencers)

        # Generate mock influence data
        influence_data = self._generate_mock_influence_data(influencers)

        # Generate stories for each influence
        stories = []
        for influence in influence_data:
            # Generate both story types for each influence
            story_types = [influence["relationship_story_type"], influence["metric_story_type"]]

            for story_type in story_types:
                time_series = self.get_mock_time_series(grain, story_type, influence)
                variables = self.get_mock_variables(metric, story_type, grain, time_series, influence)

                story = self.prepare_story_dict(
                    metric,
                    story_type,
                    grain,
                    time_series,
                    variables,
                    story_date or self.data_service.story_date,
                )
                stories.append(story)

        return stories

    def _create_mock_influencers(self, count: int) -> list[str]:
        """Create mock influencer names when none are provided"""
        # Select a random sample from our default influencers
        return random.sample(self.DEFAULT_MOCK_INFLUENCERS, min(count, len(self.DEFAULT_MOCK_INFLUENCERS)))

    def get_mock_time_series(
        self, grain: Granularity, story_type: StoryType, influence: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Generate mock time series data for influence drift stories"""
        # Get date range and dates within range
        start_date, end_date = self.data_service.get_input_time_range(grain, self.group)
        dates = self.data_service.get_dates_for_range(grain, start_date, end_date)
        formatted_dates = self.data_service.get_formatted_dates(dates)

        # Extract values from influence data
        latest_value = influence["latest_value"]  # type: ignore
        previous_value = influence["previous_value"]  # type: ignore
        output_latest = influence["output_latest"]  # type: ignore
        output_previous = influence["output_previous"]  # type: ignore

        # Generate series showing the trend
        num_points = len(formatted_dates)
        time_series = []

        for i, date_str in enumerate(formatted_dates):
            # Calculate progress in the time series
            progress = i / (num_points - 1) if num_points > 1 else 1

            # Interpolate between previous and latest values
            value = previous_value + progress * (latest_value - previous_value)
            output_value = output_previous + progress * (output_latest - output_previous)

            # Add some random noise (±5%)
            value += value * random.uniform(-0.05, 0.05)  # noqa
            output_value += output_value * random.uniform(-0.05, 0.05)  # noqa

            # Add point to time series
            time_series.append(
                {
                    "date": date_str,
                    "value": round(value),
                    "output_value": round(output_value),
                    "influence_id": influence["influence_metric_id"],  # type: ignore
                }
            )

        return time_series

    def get_mock_variables(
        self,
        metric: dict[str, Any],
        story_type: StoryType,
        grain: Granularity,
        time_series: list[dict[str, Any]] | None = None,
        influence: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Generate mock variables for influence drift stories"""
        grain_meta = GRAIN_META[grain]

        return {
            "metric": {"id": metric["id"], "label": metric["label"]},
            "grain": grain.value,
            "eoi": grain_meta["eoi"],
            "pop": grain_meta["pop"],
            "interval": grain_meta["interval"],
            "influence_metric": influence["influence_metric_id"],  # type: ignore
            "output_metric": metric["label"],
            "influence_deviation": round(abs(influence["influence_deviation"]), 2),  # type: ignore
            "output_deviation": round(abs(influence["output_deviation"]), 2),  # type: ignore
            "prev_output_deviation": round(abs(influence["prev_output_deviation"]), 2),  # type: ignore
            "movement": influence["movement"],  # type: ignore
            "pressure": influence["pressure"],  # type: ignore
            "latest_strength": round(influence["latest_strength"], 2),  # type: ignore
            "previous_strength": round(influence["previous_strength"], 2),  # type: ignore
            "current_value": round(influence["output_latest"], 2),  # type: ignore
        }

    def _generate_mock_influence_data(self, influencers: list[str]) -> list[dict[str, Any]]:
        """Generate mock data for influences"""
        influence_data = []

        # Generate output metric values
        output_latest = random.uniform(500, 2000)  # noqa
        output_previous = output_latest * random.uniform(0.8, 1.2)  # noqa
        output_prev_to_prev = output_previous * random.uniform(0.8, 1.2)  # noqa

        # Calculate output deviations
        output_deviation = ((output_latest - output_previous) / output_previous) * 100
        prev_output_deviation = ((output_previous - output_prev_to_prev) / output_prev_to_prev) * 100

        # Determine movement and pressure
        movement = Movement.INCREASE.value if output_deviation > 0 else Movement.DECREASE.value
        pressure = Pressure.UPWARD.value if output_deviation > 0 else Pressure.DOWNWARD.value

        for influence_metric_id in influencers:
            # Generate influence values
            latest_value = random.uniform(500, 2000)  # noqa
            previous_value = latest_value * random.uniform(0.8, 1.2)  # noqa

            # Calculate influence deviation
            influence_deviation = ((latest_value - previous_value) / previous_value) * 100

            # Generate relationship strengths
            # Randomly decide if relationship is getting stronger or weaker
            is_stronger = random.choice([True, False])  # noqa

            if is_stronger:
                # Relationship is getting stronger
                previous_strength = random.uniform(0.1, 0.5)  # noqa
                latest_strength = previous_strength * random.uniform(1.2, 1.5)  # noqa
                relationship_story_type = StoryType.STRONGER_INFLUENCE
            else:
                # Relationship is getting weaker
                previous_strength = random.uniform(0.5, 0.9)  # noqa
                latest_strength = previous_strength * random.uniform(0.5, 0.8)  # noqa
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
