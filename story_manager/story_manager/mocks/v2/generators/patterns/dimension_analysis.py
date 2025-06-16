"""
Dimension Analysis Pattern Result Generator
"""

import random
from typing import Any

from levers.models import (
    HistoricalPeriodRanking,
    SliceComparison,
    SlicePerformance,
    SliceRanking,
    SliceShare,
    SliceStrength,
    TopSlice,
)
from levers.models.patterns import DimensionAnalysis

from .base import PatternResultGeneratorBase


class DimensionAnalysisPatternGenerator(PatternResultGeneratorBase):
    """Generator for DimensionAnalysis pattern results"""

    def __init__(self, metric_id: str, analysis_window, dimension_name: str = "Region"):
        super().__init__(metric_id, analysis_window)
        self.dimension_name = dimension_name
        self.slice_names = self._get_slice_names()

    def _get_slice_names(self) -> list[str]:
        """Get realistic slice names based on dimension"""
        slice_options = {
            "Region": ["North America", "Europe", "Asia Pacific", "Latin America", "Middle East"],
            "Product": ["Product A", "Product B", "Product C", "Product D", "Product E"],
            "Channel": ["Online", "Retail", "Enterprise", "Partner", "Direct"],
            "Segment": ["SMB", "Mid-Market", "Enterprise", "Consumer", "Government"],
        }
        return slice_options.get(self.dimension_name, ["Slice 1", "Slice 2", "Slice 3", "Slice 4", "Slice 5"])

    def generate(self) -> DimensionAnalysis:
        """Generate a realistic DimensionAnalysis result"""

        # Generate slice performances
        slices = self._generate_slice_performances()

        # Generate top and bottom slices
        top_slices = self._generate_top_slices(slices)
        bottom_slices = self._generate_bottom_slices(slices)

        # Generate largest/smallest shares
        largest_slice = self._generate_largest_slice()
        smallest_slice = self._generate_smallest_slice()

        # Generate strongest/weakest changes
        strongest_slice = self._generate_strongest_slice()
        weakest_slice = self._generate_weakest_slice()

        # Generate comparison highlights
        comparison_highlights = self._generate_comparison_highlights()

        # Generate historical rankings
        historical_rankings = self._generate_historical_rankings()

        return DimensionAnalysis(
            metric_id=self.metric_id,
            analysis_window=self.analysis_window,
            analysis_date=self.analysis_date,
            dimension_name=self.dimension_name,
            slices=slices,
            top_slices=top_slices,
            bottom_slices=bottom_slices,
            largest_slice=largest_slice,
            smallest_slice=smallest_slice,
            strongest_slice=strongest_slice,
            weakest_slice=weakest_slice,
            comparison_highlights=comparison_highlights,
            historical_slice_rankings=historical_rankings,
        )

    def _generate_slice_performances(self) -> list[SlicePerformance]:
        """Generate realistic slice performance data"""
        performances = []

        for slice_name in self.slice_names:
            current_value = self._generate_random_value(500, 5000)
            prior_value = self._generate_realistic_variation(current_value, 20)
            prior_value = self._ensure_positive(prior_value)

            performance = SlicePerformance(
                slice_value=slice_name,
                current_value=current_value,
                prior_value=prior_value,
                absolute_change=current_value - prior_value,
                relative_change_percent=((current_value - prior_value) / prior_value) * 100,
                current_share_of_volume_percent=random.uniform(15, 25),
                prior_share_of_volume_percent=random.uniform(15, 25),
                share_of_volume_change_percent=self._generate_random_percentage(-5, 5),
                absolute_diff_percent_from_avg=self._generate_random_percentage(-20, 20),
                rank_by_performance=random.randint(1, len(self.slice_names)),
            )
            performances.append(performance)

        return performances

    def _generate_top_slices(self, slices: list[SlicePerformance]) -> list[SliceRanking]:
        """Generate top performing slices"""
        # Sort by current value and take top 3
        sorted_slices = sorted(slices, key=lambda x: x.current_value, reverse=True)[:3]

        rankings = []
        for i, slice_perf in enumerate(sorted_slices):
            ranking = SliceRanking(
                slice_value=slice_perf.slice_value,
                dimension=self.dimension_name,
                metric_value=slice_perf.current_value,
                rank=i + 1,
            )
            rankings.append(ranking)

        return rankings

    def _generate_bottom_slices(self, slices: list[SlicePerformance]) -> list[SliceRanking]:
        """Generate bottom performing slices"""
        # Sort by current value and take bottom 3
        sorted_slices = sorted(slices, key=lambda x: x.current_value)[:3]

        rankings = []
        for i, slice_perf in enumerate(sorted_slices):
            ranking = SliceRanking(
                slice_value=slice_perf.slice_value,
                dimension=self.dimension_name,
                metric_value=slice_perf.current_value,
                rank=len(self.slice_names) - i,  # Rank from bottom
            )
            rankings.append(ranking)

        return rankings

    def _generate_largest_slice(self) -> SliceShare:
        """Generate largest slice by share"""
        return SliceShare(
            slice_value=random.choice(self.slice_names),
            current_share_of_volume_percent=random.uniform(25, 35),
            previous_slice_value=random.choice(self.slice_names),
            previous_share_percent=random.uniform(20, 30),
        )

    def _generate_smallest_slice(self) -> SliceShare:
        """Generate smallest slice by share"""
        return SliceShare(
            slice_value=random.choice(self.slice_names),
            current_share_of_volume_percent=random.uniform(8, 15),
            previous_slice_value=random.choice(self.slice_names),
            previous_share_percent=random.uniform(10, 18),
        )

    def _generate_strongest_slice(self) -> SliceStrength:
        """Generate strongest performing slice"""
        current_val = self._generate_random_value(3000, 5000)
        prior_val = self._generate_random_value(2000, 3500)

        return SliceStrength(
            slice_value=random.choice(self.slice_names),
            previous_slice_value=random.choice(self.slice_names),
            current_value=current_val,
            prior_value=prior_val,
            absolute_delta=current_val - prior_val,
            relative_delta_percent=((current_val - prior_val) / prior_val) * 100,
        )

    def _generate_weakest_slice(self) -> SliceStrength:
        """Generate weakest performing slice"""
        current_val = self._generate_random_value(1000, 2500)
        prior_val = current_val * random.uniform(1.2, 1.8)

        return SliceStrength(
            slice_value=random.choice(self.slice_names),
            previous_slice_value=random.choice(self.slice_names),
            current_value=current_val,
            prior_value=prior_val,
            absolute_delta=current_val - prior_val,
            relative_delta_percent=((current_val - prior_val) / prior_val) * 100,
        )

    def _generate_comparison_highlights(self) -> list[SliceComparison]:
        """Generate slice comparison highlights"""
        comparisons = []

        # Generate 2-3 interesting comparisons
        for _ in range(random.randint(2, 3)):
            slice_a = random.choice(self.slice_names)
            slice_b = random.choice([s for s in self.slice_names if s != slice_a])

            current_val_a = self._generate_random_value(1000, 5000)
            prior_val_a = self._generate_realistic_variation(current_val_a)
            current_val_b = self._generate_random_value(1000, 5000)
            prior_val_b = self._generate_realistic_variation(current_val_b)

            comparison = SliceComparison(
                slice_a=slice_a,
                current_value_a=current_val_a,
                prior_value_a=prior_val_a,
                slice_b=slice_b,
                current_value_b=current_val_b,
                prior_value_b=prior_val_b,
                performance_gap_percent=self._generate_random_percentage(10, 50),
                gap_change_percent=self._generate_random_percentage(5, 25),
            )
            comparisons.append(comparison)

        return comparisons

    def _generate_historical_rankings(self) -> list[HistoricalPeriodRanking]:
        """Generate historical slice rankings"""
        rankings = []

        for slice_name in self.slice_names[:3]:  # Top 3 slices
            top_slices = [
                TopSlice(
                    slice_value=slice_name,
                    dimension=self.dimension_name,
                    metric_value=self._generate_random_value(2000, 5000),
                )
            ]

            ranking = HistoricalPeriodRanking(
                start_date=self._generate_date_string(90),
                end_date=self._generate_date_string(30),
                top_slices_by_performance=top_slices,
            )
            rankings.append(ranking)

        return rankings
