"""
Mock Dimension Analysis Pattern Generator

Generates mock DimensionAnalysis pattern results that simulate dimension-based analysis.
"""

import random
from datetime import date, timedelta
from typing import Any

import pandas as pd

from commons.models.enums import Granularity
from levers.models.patterns import DimensionAnalysis
from story_manager.mocks.v2.pattern_generators.base import MockPatternGeneratorBase


class MockDimensionAnalysisGenerator(MockPatternGeneratorBase):
    """Generator for mock dimension analysis pattern results."""

    pattern_name = "dimension_analysis"

    def generate_pattern_results(
        self, metric: dict[str, Any], grain: Granularity, story_date: date
    ) -> list[DimensionAnalysis]:
        """
        Generate a single mock DimensionAnalysis pattern result.

        Uses story groups approach to ensure comprehensive story coverage,
        similar to historical performance pattern.
        """
        base_data = self._get_base_pattern_data(metric, grain, story_date)

        # Use "Region" dimension consistently to match series data
        dimension = "Region"

        # Story groups approach - ensure coverage of different story types
        # Based on DimensionAnalysisEvaluator story types:
        story_groups = {
            "significant_segments": ["top4", "bottom4"],  # TOP_4_SEGMENTS, BOTTOM_4_SEGMENTS
            "segment_comparisons": ["mix_shift"],  # SEGMENT_COMPARISONS
            "segment_changes": [
                "segment_change"
            ],  # NEW_STRONGEST_SEGMENT, NEW_WEAKEST_SEGMENT, NEW_LARGEST_SEGMENT, NEW_SMALLEST_SEGMENT
        }

        # Randomly select one story type from each group to ensure comprehensive coverage
        selected_story_types = []
        for _, story_types in story_groups.items():
            selected_story_types.append(random.choice(story_types))  # noqa

        # For this single call, randomly pick one of the selected story types
        chosen_scenario = random.choice(selected_story_types)  # noqa

        # Generate the chosen scenario
        if chosen_scenario == "top4":
            scenario_data = self._generate_top4_scenario(base_data, dimension)
        elif chosen_scenario == "bottom4":
            scenario_data = self._generate_bottom4_scenario(base_data, dimension)
        elif chosen_scenario == "mix_shift":
            scenario_data = self._generate_mix_shift_scenario(base_data, dimension)
        elif chosen_scenario == "segment_change":
            scenario_data = self._generate_segment_change_scenario(base_data, dimension)
        else:
            # Fallback to top_segments for backward compatibility
            scenario_data = self._generate_top_segments_scenario(base_data, dimension)

        # Create the DimensionAnalysis instance
        result = DimensionAnalysis(**scenario_data)
        return [result]  # Return single result

    def generate_mock_series_data(
        self, metric: dict[str, Any], grain: Granularity, story_date: date, num_periods: int = 12
    ) -> pd.DataFrame:
        """
        Generate mock time series data that matches the pattern result.

        Uses the same date range logic as historical performance for consistency.

        Args:
            metric: Metric dictionary
            grain: Granularity for the series
            story_date: The story date (current period - NOT included in series)
            num_periods: Number of historical periods to generate

        Returns:
            DataFrame with mock time series data for dimension analysis
        """
        # Use EXACT same date range logic as historical performance for consistency
        if grain == Granularity.DAY:
            freq = "D"
            end_date = story_date - timedelta(days=1)
        elif grain == Granularity.WEEK:
            freq = "W-MON"
            # Calculate end_date as the Monday of the week before story_date
            days_since_monday = story_date.weekday()
            end_date = story_date - timedelta(days=days_since_monday + 7)
        else:  # MONTH
            freq = "MS"
            # For months, end_date should be first day of the month before story_date
            if story_date.month == 1:
                end_date = story_date.replace(year=story_date.year - 1, month=12, day=1)
            else:
                end_date = story_date.replace(month=story_date.month - 1, day=1)

        # Generate exactly num_periods data points ending at end_date
        date_range = pd.date_range(end=end_date, periods=num_periods, freq=freq)

        # Use the same dimension values that will be used in pattern result
        dimension_slices = self._generate_mock_dimension_values("Region")

        # Create series data in LONG format (dimension_slice column) as expected by evaluator
        series_data = []
        base_value = random.uniform(800, 1500)  # noqa

        for date_val in date_range:
            # Add date-specific variation to make each time period different
            date_variation = random.uniform(0.8, 1.2)  # noqa

            for slice_index, slice_name in enumerate(dimension_slices):
                # Create more varied multipliers with additional randomness
                base_multiplier = 1.8 - (slice_index * 0.3)  # 1.8, 1.5, 1.2, 0.9, 0.6, 0.3
                base_multiplier = max(0.3, base_multiplier)  # Ensure minimum value

                # Add significant slice-specific randomness to create unique patterns
                slice_randomness = random.uniform(0.7, 1.4)  # noqa Much more variation

                # Add time-based variation that's different for each slice
                time_factor = 1 + random.uniform(-0.15, 0.15)  # noqa

                # Combine all factors for realistic variation
                final_multiplier = base_multiplier * slice_randomness * time_factor * date_variation
                value = base_value * final_multiplier

                series_data.append(
                    {
                        "date": date_val.strftime("%Y-%m-%d"),
                        "dimension_slice": slice_name,  # This is the key column name expected by evaluator
                        "value": round(value, 2),
                    }
                )

        return pd.DataFrame(series_data)

    def _generate_top_segments_scenario(self, base_data: dict[str, Any], dimension: str) -> dict[str, Any]:
        """Generate a top performing segments scenario."""
        dimension_values = self._generate_mock_dimension_values(dimension)

        # Generate slice performance data
        slices = []
        top_slices = []
        bottom_slices = []
        total_value = self._generate_mock_value(5000)

        for i, value_name in enumerate(dimension_values):
            # Create more realistic distribution with clear above/below average segments
            if i < 2:  # Top performers - well above average
                current_value = total_value * random.uniform(0.25, 0.40)  # noqa
            elif i < 4:  # Mid performers - around average
                current_value = total_value * random.uniform(0.15, 0.25)  # noqa
            else:  # Bottom performers - below average
                current_value = total_value * random.uniform(0.05, 0.15)  # noqa

            prior_value = current_value * random.uniform(0.8, 1.2)  # noqa
            current_share = (current_value / total_value) * 100
            prior_share = (prior_value / total_value) * 100

            slice_perf = {
                "slice_value": value_name,
                "current_value": round(current_value, 2),
                "prior_value": round(prior_value, 2),
                "absolute_change": round(current_value - prior_value, 2),
                "relative_change_percent": round(((current_value - prior_value) / prior_value) * 100, 2),
                "current_share_of_volume_percent": round(current_share, 2),
                "prior_share_of_volume_percent": round(prior_share, 2),
                "share_of_volume_change_percent": round(current_share - prior_share, 2),
                "rank_by_performance": i + 1,
                "rank_by_share": i + 1,
            }
            slices.append(slice_perf)

            # Create ranking slices
            if i < 3:  # Top 3
                avg_value = total_value / len(dimension_values)
                absolute_diff_from_avg = current_value - avg_value  # type: ignore
                absolute_diff_percent_from_avg = (absolute_diff_from_avg / avg_value * 100) if avg_value != 0 else 0

                top_slices.append(
                    {
                        "slice_value": value_name,
                        "dimension": dimension,
                        "metric_value": current_value,
                        "rank": i + 1,
                        "avg_other_slices_value": avg_value,
                        "absolute_diff_from_avg": absolute_diff_from_avg,
                        "absolute_diff_percent_from_avg": absolute_diff_percent_from_avg,
                    }
                )
            elif i >= len(dimension_values) - 3:  # Bottom 3
                avg_value = total_value / len(dimension_values)
                absolute_diff_from_avg = current_value - avg_value  # type: ignore
                absolute_diff_percent_from_avg = (absolute_diff_from_avg / avg_value * 100) if avg_value != 0 else 0

                bottom_slices.append(
                    {
                        "slice_value": value_name,
                        "dimension": dimension,
                        "metric_value": current_value,
                        "rank": i + 1,
                        "avg_other_slices_value": avg_value,
                        "absolute_diff_from_avg": absolute_diff_from_avg,
                        "absolute_diff_percent_from_avg": absolute_diff_percent_from_avg,
                    }
                )

        # Generate largest and smallest slices
        largest_slice = {
            "slice_value": dimension_values[0],
            "current_share_of_volume_percent": (slices[0]["current_value"] / total_value) * 100,  # type: ignore
            "previous_slice_value": (
                dimension_values[1] if len(dimension_values) > 1 else dimension_values[0]
            ),  # Previous largest
        }

        smallest_slice = {
            "slice_value": dimension_values[-1],
            "current_share_of_volume_percent": (slices[-1]["current_value"] / total_value) * 100,  # type: ignore
            "previous_slice_value": (
                dimension_values[-2] if len(dimension_values) > 1 else dimension_values[-1]
            ),  # Previous smallest
        }

        # Generate strongest and weakest slices based on actual performance
        # Sort slices by current_value to find actual strongest and weakest
        sorted_by_performance = sorted(slices, key=lambda x: x["current_value"], reverse=True)  # type: ignore
        avg_value = total_value / len(slices)

        # Strongest slice: highest current_value
        strongest_data = sorted_by_performance[0]
        strongest_slice = {
            "slice_value": strongest_data["slice_value"],
            "current_value": strongest_data["current_value"],
            "prior_value": strongest_data["prior_value"],
            "absolute_delta": strongest_data["current_value"] - strongest_data["prior_value"],  # type: ignore
            "relative_delta_percent": (
                (
                    (strongest_data["current_value"] - strongest_data["prior_value"])  # type: ignore
                    / strongest_data["prior_value"]
                    * 100
                )
                if strongest_data["prior_value"] != 0
                else 0
            ),
            "previous_slice_value": (
                sorted_by_performance[1]["slice_value"]
                if len(sorted_by_performance) > 1
                else strongest_data["slice_value"]
            ),
        }

        # Weakest slice: lowest current_value AND below average
        weakest_candidates = [s for s in sorted_by_performance if s["current_value"] < avg_value]  # type: ignore
        if weakest_candidates:
            weakest_data = weakest_candidates[-1]  # Last (lowest) from below-average candidates
        else:
            weakest_data = sorted_by_performance[-1]  # Fallback to lowest overall

        weakest_slice = {
            "slice_value": weakest_data["slice_value"],
            "current_value": weakest_data["current_value"],
            "prior_value": weakest_data["prior_value"],
            "absolute_delta": weakest_data["current_value"] - weakest_data["prior_value"],  # type: ignore
            "relative_delta_percent": (
                ((weakest_data["current_value"] - weakest_data["prior_value"]) / weakest_data["prior_value"] * 100)  # type: ignore
                if weakest_data["prior_value"] != 0
                else 0
            ),
            "previous_slice_value": (
                sorted_by_performance[-2]["slice_value"]
                if len(sorted_by_performance) > 1
                else weakest_data["slice_value"]
            ),
        }

        return {
            **base_data,
            "dimension_name": dimension,
            "slices": slices,
            "top_slices": top_slices,
            "bottom_slices": bottom_slices,
            "largest_slice": largest_slice,
            "smallest_slice": smallest_slice,
            "strongest_slice": strongest_slice,
            "weakest_slice": weakest_slice,
        }

    def _generate_mix_shift_scenario(self, base_data: dict[str, Any], dimension: str) -> dict[str, Any]:
        """Generate a mix shift scenario."""
        dimension_values = self._generate_mock_dimension_values(dimension)

        # Generate segments with mix shift
        slices = []
        comparison_highlights = []
        total_value = self._generate_mock_value(4000)

        # Create a scenario where one segment is growing and others declining
        growing_segment = random.choice(dimension_values)  # noqa

        for value_name in dimension_values:
            is_growing = value_name == growing_segment

            if is_growing:
                current_contribution = random.uniform(0.25, 0.35)  # noqa Growing segment gets more
            else:
                current_contribution = random.uniform(0.08, 0.18)  # noqa Others get less

            previous_contribution = current_contribution * (0.8 if is_growing else 1.2)

            current_value = total_value * current_contribution
            prior_value = total_value * previous_contribution

            slices.append(
                {
                    "slice_value": value_name,
                    "current_value": round(current_value, 2),
                    "prior_value": round(prior_value, 2),
                    "absolute_change": round(current_value - prior_value, 2),
                    "relative_change_percent": round(((current_value - prior_value) / prior_value) * 100, 2),
                    "current_share_of_volume_percent": round(current_contribution * 100, 2),
                    "prior_share_of_volume_percent": round(previous_contribution * 100, 2),
                    "share_of_volume_change_percent": round((current_contribution - previous_contribution) * 100, 2),
                }
            )

        # Create comparison between growing and declining segments
        if len(slices) >= 2:
            growing_slice = next((s for s in slices if s["slice_value"] == growing_segment), slices[0])
            declining_slice = next((s for s in slices if s["slice_value"] != growing_segment), slices[1])

            # Calculate performance gap: (A - B) / B * 100
            performance_gap_percent = None
            if declining_slice["current_value"] != 0:
                performance_gap_percent = (
                    (growing_slice["current_value"] - declining_slice["current_value"])  # type: ignore
                    / declining_slice["current_value"]
                ) * 100

            # Calculate prior gap: (A_prior - B_prior) / B_prior * 100
            gap_prior = None
            if declining_slice["prior_value"] != 0:
                gap_prior = (
                    (growing_slice["prior_value"] - declining_slice["prior_value"]) / declining_slice["prior_value"]  # type: ignore
                ) * 100

            # Calculate gap change
            gap_change_percent = None
            if performance_gap_percent is not None and gap_prior is not None:
                gap_change_percent = performance_gap_percent - gap_prior

            comparison_highlights.append(
                {
                    "slice_a": growing_slice["slice_value"],
                    "current_value_a": growing_slice["current_value"],
                    "prior_value_a": growing_slice["prior_value"],
                    "slice_b": declining_slice["slice_value"],
                    "current_value_b": declining_slice["current_value"],
                    "prior_value_b": declining_slice["prior_value"],
                    "performance_gap_percent": performance_gap_percent,
                    "gap_change_percent": gap_change_percent,
                }
            )

        # Generate strongest and weakest slices based on actual performance
        # Sort slices by current_value to find actual strongest and weakest
        sorted_by_performance = sorted(slices, key=lambda x: x["current_value"], reverse=True)  # type: ignore
        avg_value = total_value / len(slices)

        # Strongest slice: highest current_value
        strongest_data = sorted_by_performance[0]
        strongest_slice = {
            "slice_value": strongest_data["slice_value"],
            "current_value": strongest_data["current_value"],
            "prior_value": strongest_data["prior_value"],
            "absolute_delta": strongest_data["current_value"] - strongest_data["prior_value"],  # type: ignore
            "relative_delta_percent": (
                (
                    (strongest_data["current_value"] - strongest_data["prior_value"])  # type: ignore
                    / strongest_data["prior_value"]
                    * 100
                )
                if strongest_data["prior_value"] != 0
                else 0
            ),
            "previous_slice_value": (
                sorted_by_performance[1]["slice_value"]
                if len(sorted_by_performance) > 1
                else strongest_data["slice_value"]
            ),
        }

        # Weakest slice: lowest current_value AND below average
        weakest_candidates = [s for s in sorted_by_performance if s["current_value"] < avg_value]  # type: ignore
        if weakest_candidates:
            weakest_data = weakest_candidates[-1]  # Last (lowest) from below-average candidates
        else:
            weakest_data = sorted_by_performance[-1]  # Fallback to lowest overall

        weakest_slice = {
            "slice_value": weakest_data["slice_value"],
            "current_value": weakest_data["current_value"],
            "prior_value": weakest_data["prior_value"],
            "absolute_delta": weakest_data["current_value"] - weakest_data["prior_value"],  # type: ignore
            "relative_delta_percent": (
                ((weakest_data["current_value"] - weakest_data["prior_value"]) / weakest_data["prior_value"] * 100)  # type: ignore
                if weakest_data["prior_value"] != 0
                else 0
            ),
            "previous_slice_value": (
                sorted_by_performance[-2]["slice_value"]
                if len(sorted_by_performance) > 1
                else weakest_data["slice_value"]
            ),
        }

        # Generate largest and smallest slices by share
        largest_slice = {
            "slice_value": max(slices, key=lambda x: x["current_share_of_volume_percent"])["slice_value"],  # type: ignore
            "current_share_of_volume_percent": max(s["current_share_of_volume_percent"] for s in slices),  # type: ignore
            "previous_slice_value": min(slices, key=lambda x: x["current_share_of_volume_percent"])["slice_value"],  # type: ignore
        }

        smallest_slice = {
            "slice_value": min(slices, key=lambda x: x["current_share_of_volume_percent"])["slice_value"],  # type: ignore
            "current_share_of_volume_percent": min(s["current_share_of_volume_percent"] for s in slices),  # type: ignore
            "previous_slice_value": max(slices, key=lambda x: x["current_share_of_volume_percent"])["slice_value"],  # type: ignore
        }

        # Generate top and bottom slices to enable all story types
        top_slices = []
        bottom_slices = []

        # Sort slices by performance to identify top and bottom
        sorted_slices = sorted(slices, key=lambda x: x["relative_change_percent"], reverse=True)  # type: ignore

        # Top 4 performers
        for i, slice_data in enumerate(sorted_slices[:4]):
            avg_value = total_value / len(slices)
            absolute_diff_from_avg = slice_data["current_value"] - avg_value  # type: ignore
            absolute_diff_percent_from_avg = (absolute_diff_from_avg / avg_value * 100) if avg_value != 0 else 0

            top_slices.append(
                {
                    "slice_value": slice_data["slice_value"],
                    "dimension": dimension,
                    "metric_value": slice_data["current_value"],
                    "rank": i + 1,
                    "avg_other_slices_value": avg_value,
                    "absolute_diff_from_avg": absolute_diff_from_avg,
                    "absolute_diff_percent_from_avg": absolute_diff_percent_from_avg,
                }
            )

        # Bottom 4 performers
        for i, slice_data in enumerate(sorted_slices[-4:]):
            avg_value = total_value / len(slices)
            absolute_diff_from_avg = slice_data["current_value"] - avg_value  # type: ignore
            absolute_diff_percent_from_avg = (absolute_diff_from_avg / avg_value * 100) if avg_value != 0 else 0

            bottom_slices.append(
                {
                    "slice_value": slice_data["slice_value"],
                    "dimension": dimension,
                    "metric_value": slice_data["current_value"],
                    "rank": len(sorted_slices) - 4 + i + 1,
                    "avg_other_slices_value": avg_value,
                    "absolute_diff_from_avg": absolute_diff_from_avg,
                    "absolute_diff_percent_from_avg": absolute_diff_percent_from_avg,
                }
            )

        return {
            **base_data,
            "dimension_name": dimension,
            "slices": slices,
            "top_slices": top_slices,
            "bottom_slices": bottom_slices,
            "comparison_highlights": comparison_highlights,
            "strongest_slice": strongest_slice,
            "weakest_slice": weakest_slice,
            "largest_slice": largest_slice,
            "smallest_slice": smallest_slice,
        }

    def _generate_segment_change_scenario(self, base_data: dict[str, Any], dimension: str) -> dict[str, Any]:
        """Generate a segment performance change scenario."""
        dimension_values = self._generate_mock_dimension_values(dimension)

        # Generate segments with varying performance changes
        slices = []
        historical_slice_rankings = []
        total_value = self._generate_mock_value(3500)

        for i, value_name in enumerate(dimension_values):
            # Some segments have significant changes, others don't
            has_significant_change = random.choice([True, False])  # noqa

            if has_significant_change:
                current_value = total_value * random.uniform(0.1, 0.25)  # noqa     # Slightly higher max
                prior_value = current_value * random.uniform(0.6, 1.4)  # noqa     # Variable change
            else:
                current_value = total_value * random.uniform(0.08, 0.20)  # noqa     # Slightly higher max
                prior_value = current_value * random.uniform(0.95, 1.05)  # noqa     # Stable

            current_share = (current_value / total_value) * 100
            prior_share = (prior_value / total_value) * 100

            slices.append(
                {
                    "slice_value": value_name,
                    "current_value": round(current_value, 2),
                    "prior_value": round(prior_value, 2),
                    "absolute_change": round(current_value - prior_value, 2),
                    "relative_change_percent": round(((current_value - prior_value) / prior_value) * 100, 2),
                    "current_share_of_volume_percent": round(current_share, 2),
                    "prior_share_of_volume_percent": round(prior_share, 2),
                    "share_of_volume_change_percent": round(current_share - prior_share, 2),
                    "rank_by_performance": i + 1,
                }
            )

        # Create historical ranking data
        historical_slice_rankings.append(
            {
                "start_date": "2024-01-01",
                "end_date": "2024-03-31",
                "top_slices_by_performance": [
                    {
                        "slice_value": slices[0]["slice_value"],
                        "dimension": dimension,
                        "metric_value": slices[0]["current_value"],
                    },
                    {
                        "slice_value": slices[1]["slice_value"] if len(slices) > 1 else slices[0]["slice_value"],
                        "dimension": dimension,
                        "metric_value": slices[1]["current_value"] if len(slices) > 1 else slices[0]["current_value"],
                    },
                ],
            }
        )

        # Generate strongest and weakest slices based on actual performance
        # Sort slices by current_value to find actual strongest and weakest
        sorted_by_performance = sorted(slices, key=lambda x: x["current_value"], reverse=True)  # type: ignore
        avg_value = total_value / len(slices)

        # Strongest slice: highest current_value
        strongest_data = sorted_by_performance[0]
        strongest_slice = {
            "slice_value": strongest_data["slice_value"],
            "current_value": strongest_data["current_value"],
            "prior_value": strongest_data["prior_value"],
            "absolute_delta": strongest_data["current_value"] - strongest_data["prior_value"],  # type: ignore
            "relative_delta_percent": (
                (
                    (strongest_data["current_value"] - strongest_data["prior_value"])  # type: ignore
                    / strongest_data["prior_value"]
                    * 100
                )
                if strongest_data["prior_value"] != 0
                else 0
            ),
            "previous_slice_value": (
                sorted_by_performance[1]["slice_value"]
                if len(sorted_by_performance) > 1
                else strongest_data["slice_value"]
            ),
        }

        # Weakest slice: lowest current_value AND below average
        weakest_candidates = [s for s in sorted_by_performance if s["current_value"] < avg_value]  # type: ignore
        if weakest_candidates:
            weakest_data = weakest_candidates[-1]  # Last (lowest) from below-average candidates
        else:
            weakest_data = sorted_by_performance[-1]  # Fallback to lowest overall

        weakest_slice = {
            "slice_value": weakest_data["slice_value"],
            "current_value": weakest_data["current_value"],
            "prior_value": weakest_data["prior_value"],
            "absolute_delta": weakest_data["current_value"] - weakest_data["prior_value"],  # type: ignore
            "relative_delta_percent": (
                ((weakest_data["current_value"] - weakest_data["prior_value"]) / weakest_data["prior_value"] * 100)  # type: ignore
                if weakest_data["prior_value"] != 0
                else 0
            ),
            "previous_slice_value": (
                sorted_by_performance[-2]["slice_value"]
                if len(sorted_by_performance) > 1
                else weakest_data["slice_value"]
            ),
        }

        # Generate largest and smallest slices by share
        largest_slice = {
            "slice_value": max(slices, key=lambda x: x["current_share_of_volume_percent"])["slice_value"],  # type: ignore
            "current_share_of_volume_percent": max(s["current_share_of_volume_percent"] for s in slices),  # type: ignore
            "previous_slice_value": min(slices, key=lambda x: x["current_share_of_volume_percent"])["slice_value"],  # type: ignore
        }

        smallest_slice = {
            "slice_value": min(slices, key=lambda x: x["current_share_of_volume_percent"])["slice_value"],  # type: ignore
            "current_share_of_volume_percent": min(s["current_share_of_volume_percent"] for s in slices),  # type: ignore
            "previous_slice_value": max(slices, key=lambda x: x["current_share_of_volume_percent"])["slice_value"],  # type: ignore
        }

        # Generate top and bottom slices to enable all story types
        top_slices = []
        bottom_slices = []
        comparison_highlights = []

        # Sort slices by performance to identify top and bottom
        sorted_slices = sorted(slices, key=lambda x: x["relative_change_percent"], reverse=True)  # type: ignore

        # Top 4 performers
        for i, slice_data in enumerate(sorted_slices[:4]):
            avg_value = total_value / len(slices)
            absolute_diff_from_avg = slice_data["current_value"] - avg_value  # type: ignore
            absolute_diff_percent_from_avg = (absolute_diff_from_avg / avg_value * 100) if avg_value != 0 else 0

            top_slices.append(
                {
                    "slice_value": slice_data["slice_value"],
                    "dimension": dimension,
                    "metric_value": slice_data["current_value"],
                    "rank": i + 1,
                    "avg_other_slices_value": avg_value,
                    "absolute_diff_from_avg": absolute_diff_from_avg,
                    "absolute_diff_percent_from_avg": absolute_diff_percent_from_avg,
                }
            )

        # Bottom 4 performers
        for i, slice_data in enumerate(sorted_slices[-4:]):
            avg_value = total_value / len(slices)
            absolute_diff_from_avg = slice_data["current_value"] - avg_value  # type: ignore
            absolute_diff_percent_from_avg = (absolute_diff_from_avg / avg_value * 100) if avg_value != 0 else 0

            bottom_slices.append(
                {
                    "slice_value": slice_data["slice_value"],
                    "dimension": dimension,
                    "metric_value": slice_data["current_value"],
                    "rank": len(sorted_slices) - 4 + i + 1,
                    "avg_other_slices_value": avg_value,
                    "absolute_diff_from_avg": absolute_diff_from_avg,
                    "absolute_diff_percent_from_avg": absolute_diff_percent_from_avg,
                }
            )

        # Generate comparison highlights for segment comparison stories
        if len(sorted_slices) >= 2:
            slice_a = sorted_slices[0]  # Top performer
            slice_b = sorted_slices[-1]  # Bottom performer

            # Calculate performance gap: (A - B) / B * 100
            performance_gap_percent = None
            if slice_b["current_value"] != 0:
                performance_gap_percent = (
                    (slice_a["current_value"] - slice_b["current_value"]) / slice_b["current_value"]  # type: ignore
                ) * 100

            # Calculate prior gap: (A_prior - B_prior) / B_prior * 100
            gap_prior = None
            if slice_b["prior_value"] != 0:
                gap_prior = ((slice_a["prior_value"] - slice_b["prior_value"]) / slice_b["prior_value"]) * 100  # type: ignore

            # Calculate gap change
            gap_change_percent = None
            if performance_gap_percent is not None and gap_prior is not None:
                gap_change_percent = performance_gap_percent - gap_prior

            comparison_highlights.append(
                {
                    "slice_a": slice_a["slice_value"],
                    "current_value_a": slice_a["current_value"],
                    "prior_value_a": slice_a["prior_value"],
                    "slice_b": slice_b["slice_value"],
                    "current_value_b": slice_b["current_value"],
                    "prior_value_b": slice_b["prior_value"],
                    "performance_gap_percent": performance_gap_percent,
                    "gap_change_percent": gap_change_percent,
                }
            )

        return {
            **base_data,
            "dimension_name": dimension,
            "slices": slices,
            "top_slices": top_slices,
            "bottom_slices": bottom_slices,
            "comparison_highlights": comparison_highlights,
            "historical_slice_rankings": historical_slice_rankings,
            "strongest_slice": strongest_slice,
            "weakest_slice": weakest_slice,
            "largest_slice": largest_slice,
            "smallest_slice": smallest_slice,
        }

    def _generate_top4_scenario(self, base_data: dict[str, Any], dimension: str) -> dict[str, Any]:
        """Generate a top 4 performing segments scenario that enables all story types."""
        dimension_values = self._generate_mock_dimension_values(dimension)

        # Generate slices for comprehensive story coverage
        slices = []
        top_slices = []
        bottom_slices = []
        comparison_highlights = []
        total_value = self._generate_mock_value(6000)

        # STEP 1: Generate values ensuring top 4 are above average and bottom 2 are below
        # For 6 segments: we want top 4 (indices 0-3) above average, bottom 2 (indices 4-5) below

        # Calculate what the average should be
        target_average = total_value / len(dimension_values)

        # Generate values for each segment
        segment_values = []
        for i, _ in enumerate(dimension_values):
            if i < 4:  # Top 4 performers - MUST be above average
                # Ensure these are significantly above average
                min_value = target_average * 1.2  # At least 20% above average
                max_value = target_average * 2.0  # Up to 100% above average
                current_value = random.uniform(min_value, max_value)  # noqa
            else:  # Bottom 2 performers - MUST be below average
                # Ensure these are significantly below average
                min_value = target_average * 0.3  # Down to 30% of average
                max_value = target_average * 0.8  # Up to 80% of average
                current_value = random.uniform(min_value, max_value)  # noqa

            segment_values.append(current_value)

        # STEP 2: Verify and adjust if needed to ensure mathematical correctness
        actual_total = sum(segment_values)
        actual_average = actual_total / len(segment_values)

        # Check if top 4 are actually above average and bottom 2 below
        top_4_values = segment_values[:4]
        bottom_2_values = segment_values[4:]

        # If any top 4 are below average or any bottom 2 are above, adjust
        if any(v <= actual_average for v in top_4_values) or any(v >= actual_average for v in bottom_2_values):
            # Recalculate with stricter bounds
            for i in range(len(segment_values)):
                if i < 4:  # Top 4 - make them higher
                    segment_values[i] = actual_average * random.uniform(1.3, 2.2)  # noqa
                else:  # Bottom 2 - make them lower
                    segment_values[i] = actual_average * random.uniform(0.2, 0.7)  # noqa

        # STEP 3: Create slice objects with the corrected values
        for i, value_name in enumerate(dimension_values):
            current_value = segment_values[i]

            if i < 4:  # Top performers
                prior_value = current_value * random.uniform(0.85, 0.95)  # noqa     # Strong growth
            else:  # Bottom performers
                prior_value = current_value * random.uniform(1.05, 1.20)  # noqa    # Declining performance

            current_share = (current_value / total_value) * 100
            prior_share = (prior_value / total_value) * 100

            slice_perf = {
                "slice_value": value_name,
                "current_value": round(current_value, 2),
                "prior_value": round(prior_value, 2),
                "absolute_change": round(current_value - prior_value, 2),
                "relative_change_percent": round(((current_value - prior_value) / prior_value) * 100, 2),
                "current_share_of_volume_percent": round(current_share, 2),
                "prior_share_of_volume_percent": round(prior_share, 2),
                "share_of_volume_change_percent": round(current_share - prior_share, 2),
                "rank_by_performance": i + 1,
                "rank_by_share": i + 1,
            }
            slices.append(slice_perf)

            # Calculate the ACTUAL average based on the final values
            actual_total = sum(s["current_value"] for s in slices)  # type: ignore
            actual_avg_value = actual_total / len(slices)

            # Top 4 slices
            if i < 4:
                absolute_diff_from_avg = current_value - actual_avg_value
                absolute_diff_percent_from_avg = (
                    (absolute_diff_from_avg / actual_avg_value * 100) if actual_avg_value != 0 else 0
                )

                top_slices.append(
                    {
                        "slice_value": value_name,
                        "dimension": dimension,
                        "metric_value": current_value,
                        "rank": i + 1,
                        "avg_other_slices_value": actual_avg_value,
                        "absolute_diff_from_avg": absolute_diff_from_avg,
                        "absolute_diff_percent_from_avg": absolute_diff_percent_from_avg,
                    }
                )

            # Bottom slices (last 2)
            if i >= len(dimension_values) - 2:
                absolute_diff_from_avg = current_value - actual_avg_value
                absolute_diff_percent_from_avg = (
                    (absolute_diff_from_avg / actual_avg_value * 100) if actual_avg_value != 0 else 0
                )

                bottom_slices.append(
                    {
                        "slice_value": value_name,
                        "dimension": dimension,
                        "metric_value": current_value,
                        "rank": i + 1,
                        "avg_other_slices_value": actual_avg_value,
                        "absolute_diff_from_avg": absolute_diff_from_avg,
                        "absolute_diff_percent_from_avg": absolute_diff_percent_from_avg,
                    }
                )

        # Generate comparison highlights for segment comparison stories with more variation
        if len(slices) >= 2:
            # Pick different pairs for more variety
            slice_a = random.choice(slices[:3])  # noqa Pick from top 3
            slice_b = random.choice(slices[3:])  # noqa Pick from bottom 3

            # Calculate performance gap: (A - B) / B * 100
            performance_gap_percent = None
            if slice_b["current_value"] != 0:
                performance_gap_percent = (
                    (slice_a["current_value"] - slice_b["current_value"]) / slice_b["current_value"]  # type: ignore
                ) * 100

            # Calculate prior gap: (A_prior - B_prior) / B_prior * 100
            gap_prior = None
            if slice_b["prior_value"] != 0:
                gap_prior = ((slice_a["prior_value"] - slice_b["prior_value"]) / slice_b["prior_value"]) * 100  # type: ignore

            # Calculate gap change
            gap_change_percent = None
            if performance_gap_percent is not None and gap_prior is not None:
                gap_change_percent = performance_gap_percent - gap_prior

            comparison_highlights.append(
                {
                    "slice_a": slice_a["slice_value"],
                    "current_value_a": slice_a["current_value"],
                    "prior_value_a": slice_a["prior_value"],
                    "slice_b": slice_b["slice_value"],
                    "current_value_b": slice_b["current_value"],
                    "prior_value_b": slice_b["prior_value"],
                    "performance_gap_percent": performance_gap_percent,
                    "gap_change_percent": gap_change_percent,
                }
            )

        # Generate largest and smallest slices
        largest_slice = {
            "slice_value": slices[0]["slice_value"],
            "current_share_of_volume_percent": slices[0]["current_share_of_volume_percent"],
            "previous_slice_value": slices[1]["slice_value"] if len(slices) > 1 else slices[0]["slice_value"],
        }

        smallest_slice = {
            "slice_value": slices[-1]["slice_value"],
            "current_share_of_volume_percent": slices[-1]["current_share_of_volume_percent"],
            "previous_slice_value": slices[-2]["slice_value"] if len(slices) > 1 else slices[-1]["slice_value"],
        }

        # Generate strongest and weakest slices based on actual performance
        # Sort slices by current_value to find actual strongest and weakest
        sorted_by_performance = sorted(slices, key=lambda x: x["current_value"], reverse=True)  # type: ignore
        avg_value = total_value / len(slices)

        # Strongest slice: highest current_value
        strongest_data = sorted_by_performance[0]
        strongest_slice = {
            "slice_value": strongest_data["slice_value"],
            "current_value": strongest_data["current_value"],
            "prior_value": strongest_data["prior_value"],
            "absolute_delta": strongest_data["current_value"] - strongest_data["prior_value"],  # type: ignore
            "relative_delta_percent": (
                (
                    (strongest_data["current_value"] - strongest_data["prior_value"])  # type: ignore
                    / strongest_data["prior_value"]
                    * 100
                )
                if strongest_data["prior_value"] != 0
                else 0
            ),
            "previous_slice_value": (
                sorted_by_performance[1]["slice_value"]
                if len(sorted_by_performance) > 1
                else strongest_data["slice_value"]
            ),
        }

        # Weakest slice: lowest current_value AND below average
        weakest_candidates = [s for s in sorted_by_performance if s["current_value"] < avg_value]  # type: ignore
        if weakest_candidates:
            weakest_data = weakest_candidates[-1]  # Last (lowest) from below-average candidates
        else:
            weakest_data = sorted_by_performance[-1]  # Fallback to lowest overall

        weakest_slice = {
            "slice_value": weakest_data["slice_value"],
            "current_value": weakest_data["current_value"],
            "prior_value": weakest_data["prior_value"],
            "absolute_delta": weakest_data["current_value"] - weakest_data["prior_value"],  # type: ignore
            "relative_delta_percent": (
                ((weakest_data["current_value"] - weakest_data["prior_value"]) / weakest_data["prior_value"] * 100)  # type: ignore
                if weakest_data["prior_value"] != 0
                else 0
            ),
            "previous_slice_value": (
                sorted_by_performance[-2]["slice_value"]
                if len(sorted_by_performance) > 1
                else weakest_data["slice_value"]
            ),
        }

        return {
            **base_data,
            "dimension_name": dimension,
            "slices": slices,
            "top_slices": top_slices,
            "bottom_slices": bottom_slices,
            "comparison_highlights": comparison_highlights,
            "largest_slice": largest_slice,
            "smallest_slice": smallest_slice,
            "strongest_slice": strongest_slice,
            "weakest_slice": weakest_slice,
        }

    def _generate_bottom4_scenario(self, base_data: dict[str, Any], dimension: str) -> dict[str, Any]:
        """Generate a bottom 4 performing segments scenario that enables all story types."""
        dimension_values = self._generate_mock_dimension_values(dimension)

        # Generate slices for comprehensive story coverage
        slices = []
        top_slices = []
        bottom_slices = []
        comparison_highlights = []
        total_value = self._generate_mock_value(4000)

        # STEP 1: Generate values ensuring bottom 4 are below average and top 2 are above
        # For 6 segments: we want top 2 (indices 0-1) above average, bottom 4 (indices 2-5) below

        # Calculate what the average should be
        target_average = total_value / len(dimension_values)

        # Generate values for each segment
        segment_values = []
        for i, _ in enumerate(dimension_values):
            if i < 2:  # Top 2 performers - MUST be above average
                # Ensure these are significantly above average
                min_value = target_average * 1.5  # At least 50% above average
                max_value = target_average * 3.0  # Up to 200% above average
                current_value = random.uniform(min_value, max_value)  # noqa
            else:  # Bottom 4 performers - MUST be below average
                # Ensure these are significantly below average
                min_value = target_average * 0.2  # Down to 20% of average
                max_value = target_average * 0.7  # Up to 70% of average
                current_value = random.uniform(min_value, max_value)  # noqa

            segment_values.append(current_value)

        # STEP 2: Verify and adjust if needed to ensure mathematical correctness
        actual_total = sum(segment_values)
        actual_average = actual_total / len(segment_values)

        # Check if top 2 are actually above average and bottom 4 below
        top_2_values = segment_values[:2]
        bottom_4_values = segment_values[2:]

        # If any top 2 are below average or any bottom 4 are above, adjust
        if any(v <= actual_average for v in top_2_values) or any(v >= actual_average for v in bottom_4_values):
            # Recalculate with stricter bounds
            for i in range(len(segment_values)):
                if i < 2:  # Top 2 - make them much higher
                    segment_values[i] = actual_average * random.uniform(2.0, 3.5)  # noqa
                else:  # Bottom 4 - make them lower
                    segment_values[i] = actual_average * random.uniform(0.1, 0.6)  # noqa

        # STEP 3: Create slice objects with the corrected values
        for i, value_name in enumerate(dimension_values):
            current_value = segment_values[i]

            if i < 2:  # Top performers
                prior_value = current_value * random.uniform(0.85, 0.95)  # noqa Strong growth
            else:  # Bottom performers
                prior_value = current_value * random.uniform(1.05, 1.25)  # noqa Declining performance

            current_share = (current_value / total_value) * 100  # type: ignore
            prior_share = (prior_value / total_value) * 100

            slice_perf = {
                "slice_value": value_name,
                "current_value": round(current_value, 2),
                "prior_value": round(prior_value, 2),
                "absolute_change": round(current_value - prior_value, 2),
                "relative_change_percent": round(((current_value - prior_value) / prior_value) * 100, 2),
                "current_share_of_volume_percent": round(current_share, 2),
                "prior_share_of_volume_percent": round(prior_share, 2),
                "share_of_volume_change_percent": round(current_share - prior_share, 2),
                "rank_by_performance": i + 1,
                "rank_by_share": i + 1,
            }
            slices.append(slice_perf)

            # Calculate the ACTUAL average based on the final values
            actual_total = sum(s["current_value"] for s in slices)  # type: ignore
            actual_avg_value = actual_total / len(slices)

            # Top slices (first 2)
            if i < 2:
                absolute_diff_from_avg = current_value - actual_avg_value
                absolute_diff_percent_from_avg = (
                    (absolute_diff_from_avg / actual_avg_value * 100) if actual_avg_value != 0 else 0
                )

                top_slices.append(
                    {
                        "slice_value": value_name,
                        "dimension": dimension,
                        "metric_value": current_value,
                        "rank": i + 1,
                        "avg_other_slices_value": actual_avg_value,
                        "absolute_diff_from_avg": absolute_diff_from_avg,
                        "absolute_diff_percent_from_avg": absolute_diff_percent_from_avg,
                    }
                )

            # Bottom 4 slices
            if i >= 2:
                absolute_diff_from_avg = current_value - actual_avg_value
                absolute_diff_percent_from_avg = (
                    (absolute_diff_from_avg / actual_avg_value * 100) if actual_avg_value != 0 else 0
                )

                bottom_slices.append(
                    {
                        "slice_value": value_name,
                        "dimension": dimension,
                        "metric_value": current_value,
                        "rank": i + 1,
                        "avg_other_slices_value": actual_avg_value,
                        "absolute_diff_from_avg": absolute_diff_from_avg,
                        "absolute_diff_percent_from_avg": absolute_diff_percent_from_avg,
                    }
                )

        # Generate comparison highlights for segment comparison stories with more variation
        if len(slices) >= 2:
            # Pick different pairs for more variety
            slice_a = random.choice(slices[:2])  # noqa Pick from top 2
            slice_b = random.choice(slices[2:])  # noqa Pick from bottom 4

            # Calculate performance gap: (A - B) / B * 100
            performance_gap_percent = None
            if slice_b["current_value"] != 0:
                performance_gap_percent = (
                    (slice_a["current_value"] - slice_b["current_value"]) / slice_b["current_value"]  # type: ignore
                ) * 100

            # Calculate prior gap: (A_prior - B_prior) / B_prior * 100
            gap_prior = None
            if slice_b["prior_value"] != 0:
                gap_prior = ((slice_a["prior_value"] - slice_b["prior_value"]) / slice_b["prior_value"]) * 100  # type: ignore

            # Calculate gap change
            gap_change_percent = None
            if performance_gap_percent is not None and gap_prior is not None:
                gap_change_percent = performance_gap_percent - gap_prior

            comparison_highlights.append(
                {
                    "slice_a": slice_a["slice_value"],
                    "current_value_a": slice_a["current_value"],
                    "prior_value_a": slice_a["prior_value"],
                    "slice_b": slice_b["slice_value"],
                    "current_value_b": slice_b["current_value"],
                    "prior_value_b": slice_b["prior_value"],
                    "performance_gap_percent": performance_gap_percent,
                    "gap_change_percent": gap_change_percent,
                }
            )

        # Generate largest and smallest slices
        largest_slice = {
            "slice_value": slices[0]["slice_value"],
            "current_share_of_volume_percent": slices[0]["current_share_of_volume_percent"],
            "previous_slice_value": slices[1]["slice_value"] if len(slices) > 1 else slices[0]["slice_value"],
        }

        smallest_slice = {
            "slice_value": slices[-1]["slice_value"],
            "current_share_of_volume_percent": slices[-1]["current_share_of_volume_percent"],
            "previous_slice_value": slices[-2]["slice_value"] if len(slices) > 1 else slices[-1]["slice_value"],
        }

        # Generate strongest and weakest slices based on actual performance
        # Sort slices by current_value to find actual strongest and weakest
        sorted_by_performance = sorted(slices, key=lambda x: x["current_value"], reverse=True)  # type: ignore
        avg_value = total_value / len(slices)

        # Strongest slice: highest current_value
        strongest_data = sorted_by_performance[0]
        strongest_slice = {
            "slice_value": strongest_data["slice_value"],
            "current_value": strongest_data["current_value"],
            "prior_value": strongest_data["prior_value"],
            "absolute_delta": strongest_data["current_value"] - strongest_data["prior_value"],  # type: ignore
            "relative_delta_percent": (
                (
                    (strongest_data["current_value"] - strongest_data["prior_value"])  # type: ignore
                    / strongest_data["prior_value"]
                    * 100
                )
                if strongest_data["prior_value"] != 0
                else 0
            ),
            "previous_slice_value": (
                sorted_by_performance[1]["slice_value"]
                if len(sorted_by_performance) > 1
                else strongest_data["slice_value"]
            ),
        }

        # Weakest slice: lowest current_value AND below average
        weakest_candidates = [s for s in sorted_by_performance if s["current_value"] < avg_value]  # type: ignore
        if weakest_candidates:
            weakest_data = weakest_candidates[-1]  # Last (lowest) from below-average candidates
        else:
            weakest_data = sorted_by_performance[-1]  # Fallback to lowest overall

        weakest_slice = {
            "slice_value": weakest_data["slice_value"],
            "current_value": weakest_data["current_value"],
            "prior_value": weakest_data["prior_value"],
            "absolute_delta": weakest_data["current_value"] - weakest_data["prior_value"],  # type: ignore
            "relative_delta_percent": (
                ((weakest_data["current_value"] - weakest_data["prior_value"]) / weakest_data["prior_value"] * 100)  # type: ignore
                if weakest_data["prior_value"] != 0
                else 0
            ),
            "previous_slice_value": (
                sorted_by_performance[-2]["slice_value"]
                if len(sorted_by_performance) > 1
                else weakest_data["slice_value"]
            ),
        }

        return {
            **base_data,
            "dimension_name": dimension,
            "slices": slices,
            "top_slices": top_slices,
            "bottom_slices": bottom_slices,
            "comparison_highlights": comparison_highlights,
            "largest_slice": largest_slice,
            "smallest_slice": smallest_slice,
            "strongest_slice": strongest_slice,
            "weakest_slice": weakest_slice,
        }
