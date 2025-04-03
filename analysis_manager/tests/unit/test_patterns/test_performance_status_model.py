"""Tests for the performance status pattern model."""

from datetime import date
from unittest.mock import MagicMock

import pytest

from analysis_manager.patterns.models.performance_status import PerformanceStatus
from levers.models.common import AnalysisWindow, Granularity
from levers.models.patterns.performance_status import (
    HoldSteady,
    MetricGVAStatus,
    MetricPerformance,
    StatusChange,
    Streak,
)


class MockPerformanceStatus(MagicMock):
    """Mock performance status class for testing."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for key, value in kwargs.items():
            setattr(self, key, value)

    def model_dump(self):
        """Return model as dict."""
        return {k: v for k, v in self.__dict__.items() if not k.startswith("_") and k != "method_calls"}

    def to_pattern_model(self):
        """Convert to pattern model."""
        return MetricPerformance(
            metric_id=self.metric_id,
            pattern=self.pattern,
            current_value=self.current_value,
            prior_value=self.prior_value,
            target_value=self.target_value,
            status=self.status,
            absolute_delta_from_prior=self.absolute_delta_from_prior,
            pop_change_percent=self.pop_change_percent,
            absolute_gap=self.absolute_gap,
            percent_gap=self.percent_gap,
            absolute_over_performance=getattr(self, "absolute_over_performance", None),
            percent_over_performance=getattr(self, "percent_over_performance", None),
            status_change=getattr(self, "status_change", None),
            streak=getattr(self, "streak", None),
            hold_steady=getattr(self, "hold_steady", None),
            analysis_date=self.analysis_date,
            analysis_window=self.analysis_window,
        )

    @property
    def pattern_model_class(self):
        """Get pattern model class."""
        return MetricPerformance


@pytest.fixture
def performance_status_model():
    """Create a performance status model."""
    return PerformanceStatus(
        id=1,
        metric_id="test_metric",
        pattern="performance_status",
        version="1.0.0",
        analysis_window=AnalysisWindow(grain=Granularity.DAY, start_date="2023-01-01", end_date="2023-01-31"),
        analysis_date=date(2023, 2, 1),
        tenant_id="test_tenant",
        current_value=100.0,
        prior_value=90.0,
        target_value=110.0,
        status=MetricGVAStatus.ON_TRACK,
        absolute_delta_from_prior=10.0,
        pop_change_percent=0.11,
        absolute_gap=10.0,
        percent_gap=0.09,
    )


@pytest.fixture
def performance_status_with_extras():
    """Create a performance status model with all optional fields."""
    return PerformanceStatus(
        id=1,
        metric_id="test_metric",
        pattern="performance_status",
        version="1.0.0",
        analysis_window=AnalysisWindow(grain=Granularity.DAY, start_date="2023-01-01", end_date="2023-01-31"),
        analysis_date=date(2023, 2, 1),
        tenant_id="test_tenant",
        current_value=120.0,
        prior_value=100.0,
        target_value=110.0,
        status=MetricGVAStatus.ON_TRACK,
        absolute_delta_from_prior=20.0,
        pop_change_percent=0.2,
        absolute_over_performance=10.0,
        percent_over_performance=0.09,
        status_change=StatusChange(
            old_status=MetricGVAStatus.OFF_TRACK,
            new_status=MetricGVAStatus.ON_TRACK,
            old_status_duration_grains=1,
            has_flipped=True,
        ),
        streak=Streak(
            status=MetricGVAStatus.ON_TRACK,
            length=3,
            performance_change_percent_over_streak=0.05,
            absolute_change_over_streak=10.0,
            average_change_percent_per_grain=0.01,
            average_change_absolute_per_grain=1.0,
        ),
        hold_steady=HoldSteady(
            is_currently_at_or_above_target=True, time_to_maintain_grains=1, current_margin_percent=0.05
        ),
    )


class TestPerformanceStatusModel:
    """Tests for the PerformanceStatus class."""

    def test_basic_attributes(self, performance_status_model):
        """Test basic attributes of performance status model."""
        assert performance_status_model.metric_id == "test_metric"
        assert performance_status_model.pattern == "performance_status"
        assert performance_status_model.version == "1.0.0"
        assert performance_status_model.analysis_window.start_date == "2023-01-01"
        assert performance_status_model.analysis_window.end_date == "2023-01-31"
        assert performance_status_model.analysis_date == date(2023, 2, 1)

        # Performance status specific attributes
        assert performance_status_model.current_value == 100.0
        assert performance_status_model.prior_value == 90.0
        assert performance_status_model.target_value == 110.0
        assert performance_status_model.status == MetricGVAStatus.ON_TRACK
        assert performance_status_model.absolute_delta_from_prior == 10.0
        assert performance_status_model.pop_change_percent == 0.11
        assert performance_status_model.absolute_gap == 10.0
        assert performance_status_model.percent_gap == 0.09

        # Optional fields should be None or not present
        assert getattr(performance_status_model, "absolute_over_performance", None) is None
        assert getattr(performance_status_model, "percent_over_performance", None) is None
        assert getattr(performance_status_model, "status_change", None) is None
        assert getattr(performance_status_model, "streak", None) is None
        assert getattr(performance_status_model, "hold_steady", None) is None

    def test_optional_attributes(self, performance_status_with_extras):
        """Test optional attributes of performance status model."""
        model = performance_status_with_extras

        # Basic metrics
        assert model.current_value == 120.0
        assert model.prior_value == 100.0
        assert model.target_value == 110.0
        assert model.status == MetricGVAStatus.ON_TRACK

        # Optional fields
        assert model.absolute_over_performance == 10.0
        assert model.percent_over_performance == 0.09

        # Status change
        assert model.status_change is not None
        assert model.status_change.old_status == MetricGVAStatus.OFF_TRACK
        assert model.status_change.new_status == MetricGVAStatus.ON_TRACK
        assert model.status_change.old_status_duration_grains == 1
        assert model.status_change.has_flipped is True

        # Streak
        assert model.streak is not None
        assert model.streak.status == MetricGVAStatus.ON_TRACK
        assert model.streak.length == 3
        assert model.streak.performance_change_percent_over_streak == 0.05
        assert model.streak.absolute_change_over_streak == 10.0
        assert model.streak.average_change_percent_per_grain == 0.01
        assert model.streak.average_change_absolute_per_grain == 1.0

        # Hold steady
        assert model.hold_steady is not None
        assert model.hold_steady.is_currently_at_or_above_target is True
        assert model.hold_steady.time_to_maintain_grains == 1
        assert model.hold_steady.current_margin_percent == 0.05

    def test_to_pattern_model(self, performance_status_model):
        """Test conversion to pattern model."""
        pattern_model = performance_status_model.to_pattern_model()

        # Verify type
        assert isinstance(pattern_model, MetricPerformance)

        # Verify fields
        assert pattern_model.metric_id == "test_metric"
        assert pattern_model.pattern == "performance_status"
        assert pattern_model.current_value == 100.0
        assert pattern_model.prior_value == 90.0
        assert pattern_model.target_value == 110.0
        assert pattern_model.status == MetricGVAStatus.ON_TRACK
        assert pattern_model.absolute_delta_from_prior == 10.0
        assert pattern_model.pop_change_percent == 0.11
        assert pattern_model.absolute_gap == 10.0
        assert pattern_model.percent_gap == 0.09

    def test_pattern_model_class(self, performance_status_model):
        """Test pattern model class property."""
        assert performance_status_model.pattern_model_class == MetricPerformance
