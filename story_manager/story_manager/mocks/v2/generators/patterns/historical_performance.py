"""
Historical Performance Pattern Result Generator
"""

import random
from datetime import date, timedelta

from levers.models import ComparisonType, TrendExceptionType, TrendType
from levers.models.patterns import HistoricalPerformance
from levers.models.patterns.historical_performance import (
    Benchmark,
    BenchmarkComparison,
    GrowthStats,
    PeriodMetrics,
    RankSummary,
    Seasonality,
    TrendAnalysis,
    TrendException,
    TrendInfo,
)

from .base import PatternResultGeneratorBase


class HistoricalPerformancePatternGenerator(PatternResultGeneratorBase):
    """Generator for HistoricalPerformance pattern results"""

    def generate(self) -> HistoricalPerformance:
        """Generate a realistic HistoricalPerformance result"""

        # Generate period metrics
        period_metrics = self._generate_period_metrics()

        # Generate growth stats
        growth_stats = self._generate_growth_stats()

        # Generate trend info
        current_trend = self._generate_current_trend()
        previous_trend = self._generate_previous_trend()
        trend_analysis = self._generate_trend_analysis()

        # Generate rank summaries
        high_rank = self._generate_high_rank()
        low_rank = self._generate_low_rank()

        # Generate seasonality
        seasonality = self._generate_seasonality()

        # Generate benchmark comparison
        benchmark_comparison = self._generate_benchmark_comparison()

        # Generate trend exception (maybe)
        trend_exception = self._generate_trend_exception()

        return HistoricalPerformance(
            metric_id=self.metric_id,
            analysis_window=self.analysis_window,
            analysis_date=self.analysis_date,
            period_metrics=period_metrics,
            growth_stats=growth_stats,
            current_trend=current_trend,
            previous_trend=previous_trend,
            trend_analysis=trend_analysis,
            high_rank=high_rank,
            low_rank=low_rank,
            seasonality=seasonality,
            benchmark_comparison=benchmark_comparison,
            trend_exception=trend_exception,
        )

    def _generate_period_metrics(self) -> list[PeriodMetrics]:
        """Generate period metrics for historical analysis"""
        metrics = []

        for i in range(12):  # 12 periods
            start_date = self._generate_date_string(30 * (i + 1))
            end_date = self._generate_date_string(30 * i)

            metric = PeriodMetrics(
                period_start=start_date,
                period_end=end_date,
                pop_growth_percent=self._generate_random_percentage(-10, 25),
                pop_acceleration_percent=self._generate_random_percentage(-15, 15),
            )
            metrics.append(metric)

        return metrics

    def _generate_growth_stats(self) -> GrowthStats:
        """Generate growth statistics"""
        return GrowthStats(
            current_pop_growth=self._generate_random_percentage(-5, 30),
            average_pop_growth=self._generate_random_percentage(0, 20),
            current_growth_acceleration=self._generate_random_percentage(-10, 10),
            num_periods_accelerating=random.randint(2, 6),
            num_periods_slowing=random.randint(1, 4),
        )

    def _generate_current_trend(self) -> TrendInfo:
        """Generate current trend information"""
        trend_types = [TrendType.UPWARD, TrendType.DOWNWARD, TrendType.STABLE, TrendType.PLATEAU]

        return TrendInfo(
            trend_type=random.choice(trend_types),
            start_date=self._generate_date_string(60),
            average_pop_growth=self._generate_random_percentage(-10, 20),
            duration_grains=random.randint(6, 24),
        )

    def _generate_previous_trend(self) -> TrendInfo:
        """Generate previous trend information"""
        trend_types = [TrendType.UPWARD, TrendType.DOWNWARD, TrendType.STABLE]

        return TrendInfo(
            trend_type=random.choice(trend_types),
            start_date=self._generate_date_string(150),
            average_pop_growth=self._generate_random_percentage(-15, 15),
            duration_grains=random.randint(12, 36),
        )

    def _generate_trend_analysis(self) -> list[TrendAnalysis]:
        """Generate trend analysis data points"""
        analysis_points = []

        for i in range(20):  # 20 data points
            base_value = self._generate_random_value(1000, 5000)

            analysis = TrendAnalysis(
                value=base_value,
                date=self._generate_date_string(i * 7),  # Weekly data
                central_line=self._generate_realistic_variation(base_value, 5),
                ucl=base_value * random.uniform(1.05, 1.15),
                lcl=base_value * random.uniform(0.85, 0.95),
                slope=self._generate_random_percentage(-2, 3) / 100,
                slope_change_percent=self._generate_random_percentage(-5, 5),
                trend_signal_detected=random.random() > 0.8,  # 20% chance
            )
            analysis_points.append(analysis)

        return analysis_points

    def _generate_high_rank(self) -> RankSummary:
        """Generate high value rank summary"""
        current_value = self._generate_random_value(8000, 15000)
        prior_record = self._generate_random_value(7000, 12000)

        return RankSummary(
            value=current_value,
            rank=random.randint(1, 3),
            duration_grains=random.randint(30, 365),
            prior_record_value=prior_record,
            prior_record_date=self._generate_date_string(90),
            absolute_delta_from_prior_record=current_value - prior_record,
            relative_delta_from_prior_record=((current_value - prior_record) / prior_record) * 100,
        )

    def _generate_low_rank(self) -> RankSummary:
        """Generate low value rank summary"""
        current_value = self._generate_random_value(500, 1500)
        prior_record = self._generate_random_value(300, 1200)

        return RankSummary(
            value=current_value,
            rank=random.randint(1, 5),
            duration_grains=random.randint(30, 365),
            prior_record_value=prior_record,
            prior_record_date=self._generate_date_string(120),
            absolute_delta_from_prior_record=current_value - prior_record,
            relative_delta_from_prior_record=((current_value - prior_record) / prior_record) * 100,
        )

    def _generate_seasonality(self) -> Seasonality:
        """Generate seasonality information"""
        return Seasonality(
            is_following_expected_pattern=random.random() > 0.3,  # 70% following pattern
            expected_change_percent=self._generate_random_percentage(-10, 15),
            actual_change_percent=self._generate_random_percentage(-15, 20),
            deviation_percent=self._generate_random_percentage(-8, 8),
        )

    def _generate_benchmark_comparison(self) -> BenchmarkComparison:
        """Generate benchmark comparison data"""
        current_value = self._generate_random_value(2000, 8000)

        benchmarks = {}
        comparison_types = [ComparisonType.WEEK_AGO, ComparisonType.MONTH_AGO, ComparisonType.QUARTER_AGO]

        for comp_type in comparison_types:
            ref_value = self._generate_realistic_variation(current_value, 20)
            ref_value = self._ensure_positive(ref_value)

            days_back = (
                7 if comp_type == ComparisonType.WEEK_AGO else (30 if comp_type == ComparisonType.MONTH_AGO else 90)
            )

            benchmark = Benchmark(
                reference_value=ref_value,
                reference_date=date.today() - timedelta(days=days_back),
                reference_period=comp_type.value,
                absolute_change=current_value - ref_value,
                change_percent=((current_value - ref_value) / ref_value) * 100,
            )
            benchmarks[comp_type] = benchmark

        return BenchmarkComparison(
            current_value=current_value,
            current_period="This Week",
            benchmarks=benchmarks,
        )

    def _generate_trend_exception(self) -> TrendException | None:
        """Generate trend exception (spike or drop)"""
        if random.random() > 0.7:  # 30% chance of exception
            exception_type = random.choice([TrendExceptionType.SPIKE, TrendExceptionType.DROP])
            normal_low = self._generate_random_value(2000, 4000)
            normal_high = normal_low * 1.5

            if exception_type == TrendExceptionType.SPIKE:
                current_value = normal_high * random.uniform(1.2, 2.0)
            else:
                current_value = normal_low * random.uniform(0.3, 0.8)

            return TrendException(
                type=exception_type,
                current_value=current_value,
                normal_range_low=normal_low,
                normal_range_high=normal_high,
                absolute_delta_from_normal_range=abs(current_value - (normal_low + normal_high) / 2),
                magnitude_percent=abs(
                    ((current_value - (normal_low + normal_high) / 2) / ((normal_low + normal_high) / 2)) * 100
                ),
            )

        return None
