"""Tests for target calculator utility."""

from datetime import date

import pytest
from pytest_mock import MockFixture

from commons.models.enums import Granularity
from query_manager.semantic_manager.models import MetricTarget, TargetCalculationType
from query_manager.semantic_manager.utils.target_calculator import TargetCalculator


class TestTargetCalculator:
    """Tests for TargetCalculator utility class."""

    def test_calculate_targets_value_single_period(self, mocker: MockFixture) -> None:
        """Test calculate_targets method with VALUE calculation for a single period."""
        # Mock GrainPeriodCalculator.get_dates_for_range to return a single date
        mocker.patch(
            "query_manager.semantic_manager.utils.target_calculator.GrainPeriodCalculator.get_dates_for_range",
            return_value=[date(2024, 1, 1)],
        )

        # Execute
        result = TargetCalculator.calculate_targets(
            current_value=100.0,
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 1),
            grain=Granularity.DAY,
            calculation_type=TargetCalculationType.VALUE,
            target_value=150.0,
        )

        # Assert
        assert len(result) == 1
        assert result[0].date == date(2024, 1, 1)
        assert result[0].value == 150.0
        assert result[0].growth_percentage == 50.0  # (150-100)/100 * 100
        assert result[0].pop_growth_percentage == 0.0

    def test_calculate_targets_value_multiple_periods(self, mocker: MockFixture) -> None:
        """Test calculate_targets method with VALUE calculation for multiple periods."""
        # Mock GrainPeriodCalculator.get_dates_for_range
        mocker.patch(
            "query_manager.semantic_manager.utils.target_calculator.GrainPeriodCalculator.get_dates_for_range",
            return_value=[date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)],
        )

        # Execute
        result = TargetCalculator.calculate_targets(
            current_value=100.0,
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 3),
            grain=Granularity.DAY,
            calculation_type=TargetCalculationType.VALUE,
            target_value=190.0,
        )

        # Assert
        assert len(result) == 3
        # Steps should be evenly distributed: (190-100)/3 = 30 per step
        assert result[0].date == date(2024, 1, 1)
        assert result[0].value == 130.0  # 100 + 30
        assert result[0].growth_percentage == 30.0

        assert result[1].date == date(2024, 1, 2)
        assert result[1].value == 160.0  # 100 + 30*2
        assert result[1].growth_percentage == 60.0
        assert result[1].pop_growth_percentage == pytest.approx(23.08, 0.01)  # (160-130)/130 * 100

        assert result[2].date == date(2024, 1, 3)
        assert result[2].value == 190.0  # 100 + 30*3
        assert result[2].growth_percentage == 90.0
        assert result[2].pop_growth_percentage == pytest.approx(18.75, 0.01)  # (190-160)/160 * 100

    def test_calculate_targets_growth(self, mocker: MockFixture) -> None:
        """Test calculate_targets method with GROWTH calculation."""
        # Mock GrainPeriodCalculator.get_dates_for_range
        mocker.patch(
            "query_manager.semantic_manager.utils.target_calculator.GrainPeriodCalculator.get_dates_for_range",
            return_value=[date(2024, 1, 1), date(2024, 1, 2)],
        )

        # Execute
        result = TargetCalculator.calculate_targets(
            current_value=100.0,
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 2),
            grain=Granularity.DAY,
            calculation_type=TargetCalculationType.GROWTH,
            growth_percentage=20.0,
        )

        # Assert
        assert len(result) == 2
        assert result[0].date == date(2024, 1, 1)
        assert result[0].value == 100.0  # No growth at start point
        assert result[0].growth_percentage == 0.0

        assert result[1].date == date(2024, 1, 2)
        assert result[1].value == pytest.approx(120.0, 0.01)  # 100 * (1 + 20/100)^(1/1)
        assert result[1].growth_percentage == pytest.approx(20.0, 0.01)
        assert result[1].pop_growth_percentage == pytest.approx(20.0, 0.01)  # (120-100)/100 * 100

    def test_calculate_targets_pop_growth(self, mocker: MockFixture) -> None:
        """Test calculate_targets method with POP_GROWTH calculation."""
        # Mock GrainPeriodCalculator.get_dates_for_range
        mocker.patch(
            "query_manager.semantic_manager.utils.target_calculator.GrainPeriodCalculator.get_dates_for_range",
            return_value=[date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)],
        )

        # Execute
        result = TargetCalculator.calculate_targets(
            current_value=100.0,
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 3),
            grain=Granularity.DAY,
            calculation_type=TargetCalculationType.POP_GROWTH,
            pop_growth_percentage=10.0,
        )

        # Assert
        assert len(result) == 3
        assert result[0].date == date(2024, 1, 1)
        assert result[0].value == pytest.approx(110.0, 0.01)  # 100 * 1.1
        assert result[0].growth_percentage == pytest.approx(10.0, 0.01)
        assert result[0].pop_growth_percentage == pytest.approx(10.0, 0.01)

        assert result[1].date == date(2024, 1, 2)
        assert result[1].value == pytest.approx(121.0, 0.01)  # 110 * 1.1
        assert result[1].growth_percentage == pytest.approx(21.0, 0.01)
        assert result[1].pop_growth_percentage == pytest.approx(10.0, 0.01)

        assert result[2].date == date(2024, 1, 3)
        assert result[2].value == pytest.approx(133.1, 0.01)  # 121 * 1.1
        assert result[2].growth_percentage == pytest.approx(33.1, 0.01)
        assert result[2].pop_growth_percentage == pytest.approx(10.0, 0.01)

    def test_calculate_targets_with_negative_value(self, mocker: MockFixture) -> None:
        """Test calculate_targets method with a negative current value."""
        # Mock GrainPeriodCalculator.get_dates_for_range
        mocker.patch(
            "query_manager.semantic_manager.utils.target_calculator.GrainPeriodCalculator.get_dates_for_range",
            return_value=[date(2024, 1, 1), date(2024, 1, 2)],
        )

        # Execute - test with GROWTH
        result = TargetCalculator.calculate_targets(
            current_value=-100.0,
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 2),
            grain=Granularity.DAY,
            calculation_type=TargetCalculationType.GROWTH,
            growth_percentage=20.0,
        )

        # Assert
        assert len(result) == 2
        assert result[0].date == date(2024, 1, 1)
        assert result[0].value < 0  # Should still be negative
        assert result[0].value > -100.0  # But less negative than the starting value

        # Execute - test with POP_GROWTH
        result = TargetCalculator.calculate_targets(
            current_value=-100.0,
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 2),
            grain=Granularity.DAY,
            calculation_type=TargetCalculationType.POP_GROWTH,
            pop_growth_percentage=10.0,
        )

        # Assert
        assert len(result) == 2
        assert result[0].date == date(2024, 1, 1)
        assert result[0].value == pytest.approx(-90.91, 0.01)  # -100 / 1.1
        assert result[1].value == pytest.approx(-82.64, 0.01)  # -90.91 / 1.1

    def test_add_growth_stats_to_targets(self) -> None:
        """Test add_growth_stats_to_targets method."""
        # Create mock target objects
        targets = [
            self._create_mock_target(
                id=1, metric_id="metric1", grain=Granularity.DAY, date=date(2024, 1, 1), value=100.0
            ),
            self._create_mock_target(
                id=2, metric_id="metric1", grain=Granularity.DAY, date=date(2024, 1, 2), value=120.0
            ),
            self._create_mock_target(
                id=3, metric_id="metric1", grain=Granularity.DAY, date=date(2024, 1, 3), value=150.0
            ),
        ]

        # Execute
        result = TargetCalculator.add_growth_stats_to_targets(targets)

        # Assert
        assert len(result) == 3
        assert result[0].id == 1
        assert result[0].growth_percentage == 0.0  # First entry, growth from self
        assert result[0].pop_growth_percentage == 0.0  # First entry

        assert result[1].id == 2
        assert result[1].growth_percentage == 20.0  # (120-100)/100 * 100
        assert result[1].pop_growth_percentage == 20.0  # (120-100)/100 * 100

        assert result[2].id == 3
        assert result[2].growth_percentage == 50.0  # (150-100)/100 * 100
        assert result[2].pop_growth_percentage == 25.0  # (150-120)/120 * 100

    def test_add_growth_stats_with_multiple_metrics(self) -> None:
        """Test add_growth_stats_to_targets with multiple metrics."""
        # Create mock target objects for multiple metrics
        targets = [
            self._create_mock_target(
                id=1, metric_id="metric1", grain=Granularity.DAY, date=date(2024, 1, 1), value=100.0
            ),
            self._create_mock_target(
                id=2, metric_id="metric1", grain=Granularity.DAY, date=date(2024, 1, 2), value=120.0
            ),
            self._create_mock_target(
                id=3, metric_id="metric2", grain=Granularity.DAY, date=date(2024, 1, 1), value=200.0
            ),
            self._create_mock_target(
                id=4, metric_id="metric2", grain=Granularity.DAY, date=date(2024, 1, 2), value=220.0
            ),
        ]

        # Execute
        result = TargetCalculator.add_growth_stats_to_targets(targets)

        # Assert
        assert len(result) == 4

        # Group 1 - metric1
        assert result[0].id == 1
        assert result[0].growth_percentage == 0.0
        assert result[0].pop_growth_percentage == 0.0

        assert result[1].id == 2
        assert result[1].growth_percentage == 20.0  # (120-100)/100 * 100
        assert result[1].pop_growth_percentage == 20.0

        # Group 2 - metric2
        assert result[2].id == 3
        assert result[2].growth_percentage == 0.0
        assert result[2].pop_growth_percentage == 0.0

        assert result[3].id == 4
        assert result[3].growth_percentage == 10.0  # (220-200)/200 * 100
        assert result[3].pop_growth_percentage == 10.0

    def test_edge_cases(self, mocker: MockFixture) -> None:
        """Test various edge cases in target calculations."""
        # Test with empty dates
        mocker.patch(
            "query_manager.semantic_manager.utils.target_calculator.GrainPeriodCalculator.get_dates_for_range",
            return_value=[],
        )

        result = TargetCalculator.calculate_targets(
            current_value=100.0,
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 1),
            grain=Granularity.DAY,
            calculation_type=TargetCalculationType.VALUE,
            target_value=150.0,
        )
        assert len(result) == 0

        # Test with empty targets
        result = TargetCalculator.add_growth_stats_to_targets([])
        assert len(result) == 0

        # Test with single target
        target = self._create_mock_target(
            id=1, metric_id="metric1", grain=Granularity.DAY, date=date(2024, 1, 1), value=100.0
        )
        result = TargetCalculator.add_growth_stats_to_targets([target])
        assert len(result) == 1
        assert result[0].growth_percentage == 0.0
        assert result[0].pop_growth_percentage == 0.0

    @staticmethod
    def _create_mock_target(id: int, metric_id: str, grain: Granularity, date: date, value: float) -> MetricTarget:
        """Create mock MetricTarget object for testing."""
        return MetricTarget(
            id=id,
            tenant_id=1,
            metric_id=metric_id,
            grain=grain,
            target_date=date,
            target_value=value,
            target_upper_bound=value * 1.1,
            target_lower_bound=value * 0.9,
            yellow_buffer=5.0,
            red_buffer=10.0,
            created_at=None,
            updated_at=None,
        )
