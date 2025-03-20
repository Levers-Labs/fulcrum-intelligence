"""Tests for semantic manager models."""

from datetime import date, datetime

import pytest
from pydantic import ValidationError

from commons.models.enums import Granularity
from query_manager.semantic_manager.models import (
    MetricDimensionalTimeSeries,
    MetricSyncStatus,
    MetricTarget,
    MetricTimeSeries,
    SyncEvent,
    SyncStatus,
    SyncType,
)


def test_metric_time_series_model():
    """Test MetricTimeSeries model validation."""
    # Test valid model
    model = MetricTimeSeries(
        tenant_id=1,
        metric_id="test_metric",
        date=date(2024, 1, 1),
        grain=Granularity.DAY,
        value=100.0,
    )
    assert model.tenant_id == 1
    assert model.metric_id == "test_metric"
    assert model.date == date(2024, 1, 1)
    assert model.grain == Granularity.DAY
    assert model.value == 100.0

    # Test invalid value type
    with pytest.raises(ValidationError):
        MetricTimeSeries.model_validate(
            dict(
                tenant_id=1,
                metric_id="test_metric",
                date=date(2024, 1, 1),
                grain=Granularity.DAY,
                value="invalid",  # type: ignore
            )
        )

    # Test missing required fields
    with pytest.raises(ValidationError):
        MetricTimeSeries.model_validate(
            dict(
                tenant_id=1,
                metric_id="test_metric",
            )
        )  # type: ignore


def test_metric_dimensional_time_series_model():
    """Test MetricDimensionalTimeSeries model validation."""
    # Test valid model
    model = MetricDimensionalTimeSeries(
        tenant_id=1,
        metric_id="test_metric",
        date=date(2024, 1, 1),
        grain=Granularity.DAY,
        dimension_name="region",
        dimension_slice="US",
        value=100.0,
    )
    assert model.tenant_id == 1
    assert model.metric_id == "test_metric"
    assert model.date == date(2024, 1, 1)
    assert model.grain == Granularity.DAY
    assert model.dimension_name == "region"
    assert model.dimension_slice == "US"
    assert model.value == 100.0

    # Test optional dimension_slice
    model = MetricDimensionalTimeSeries(
        tenant_id=1,
        metric_id="test_metric",
        date=date(2024, 1, 1),
        grain=Granularity.DAY,
        dimension_name="region",
        dimension_slice=None,
        value=100.0,
    )
    assert model.dimension_slice is None

    # Test invalid value type
    with pytest.raises(ValidationError):
        MetricDimensionalTimeSeries.model_validate(
            dict(
                tenant_id=1,
                metric_id="test_metric",
                date=date(2024, 1, 1),
                grain=Granularity.DAY,
                dimension_name="region",
                dimension_slice="US",
                value="invalid",  # type: ignore
            )
        )


def test_metric_sync_status_model():
    """Test MetricSyncStatus model validation."""
    now = datetime.now()
    # Test valid model
    model = MetricSyncStatus(
        tenant_id=1,
        metric_id="test_metric",
        grain=Granularity.DAY,
        dimension_name="region",
        last_sync_at=now,
        sync_status=SyncStatus.SUCCESS,
        sync_type=SyncType.FULL,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 2),
        records_processed=100,
        error=None,
        history=[],
    )
    assert model.tenant_id == 1
    assert model.metric_id == "test_metric"
    assert model.grain == Granularity.DAY
    assert model.dimension_name == "region"
    assert model.last_sync_at == now
    assert model.sync_status == SyncStatus.SUCCESS
    assert model.sync_type == SyncType.FULL
    assert model.start_date == date(2024, 1, 1)
    assert model.end_date == date(2024, 1, 2)
    assert model.records_processed == 100
    assert model.error is None
    assert model.history == []

    # Test with history
    history_entry: SyncEvent = {
        "sync_status": SyncStatus.RUNNING,
        "sync_type": SyncType.FULL,
        "start_date": "2024-01-01",
        "end_date": "2024-01-02",
        "records_processed": None,
        "error": None,
        "last_sync_at": now.isoformat(),
        "updated_at": now.isoformat(),
    }
    model = MetricSyncStatus(
        tenant_id=1,
        metric_id="test_metric",
        grain=Granularity.DAY,
        dimension_name="region",
        last_sync_at=now,
        sync_status=SyncStatus.SUCCESS,
        sync_type=SyncType.FULL,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 2),
        records_processed=100,
        error=None,
        history=[history_entry],
    )
    assert len(model.history) == 1
    assert model.history[0]["sync_status"] == SyncStatus.RUNNING

    # Test invalid sync status
    with pytest.raises(ValidationError):
        MetricSyncStatus.model_validate(
            dict(
                tenant_id=1,
                metric_id="test_metric",
                grain=Granularity.DAY,
                dimension_name="region",
                last_sync_at=now,
                sync_status="INVALID",  # type: ignore
                sync_type=SyncType.FULL,
                start_date=date(2024, 1, 1),
                end_date=date(2024, 1, 2),
            )
        )

    # Test invalid sync type
    with pytest.raises(ValidationError):
        MetricSyncStatus.model_validate(
            dict(
                tenant_id=1,
                metric_id="test_metric",
                grain=Granularity.DAY,
                dimension_name="region",
                last_sync_at=now,
                sync_status=SyncStatus.SUCCESS,
                sync_type="INVALID",  # type: ignore
                start_date=date(2024, 1, 1),
                end_date=date(2024, 1, 2),
            )
        )


def test_sync_event_type():
    """Test SyncEvent type validation."""
    now = datetime.now()
    # Test valid sync event
    event: SyncEvent = {
        "sync_status": SyncStatus.SUCCESS,
        "sync_type": SyncType.FULL,
        "start_date": "2024-01-01",
        "end_date": "2024-01-02",
        "records_processed": 100,
        "error": None,
        "last_sync_at": now.isoformat(),
        "updated_at": now.isoformat(),
    }

    assert event["sync_status"] == SyncStatus.SUCCESS
    assert event["sync_type"] == SyncType.FULL
    assert event["start_date"] == "2024-01-01"
    assert event["end_date"] == "2024-01-02"
    assert event["records_processed"] == 100
    assert event["error"] is None
    assert isinstance(event["last_sync_at"], str)
    assert isinstance(event["updated_at"], str)

    # Test with error
    event = {
        "sync_status": SyncStatus.FAILED,
        "sync_type": SyncType.FULL,
        "start_date": "2024-01-01",
        "end_date": "2024-01-02",
        "records_processed": None,
        "error": "Sync failed due to network error",
        "last_sync_at": now.isoformat(),
        "updated_at": now.isoformat(),
    }

    assert event["sync_status"] == SyncStatus.FAILED
    assert event["error"] == "Sync failed due to network error"
    assert event["records_processed"] is None


def test_metric_target_model():
    """Test MetricTarget model validation."""
    # Test valid model
    model = MetricTarget(
        tenant_id=1,
        metric_id="test_metric",
        grain=Granularity.DAY,
        target_date=date(2024, 1, 1),
        target_value=100.0,
        target_upper_bound=110.0,
        target_lower_bound=90.0,
        yellow_buffer=5.0,
        red_buffer=10.0,
    )
    assert model.tenant_id == 1
    assert model.metric_id == "test_metric"
    assert model.grain == Granularity.DAY
    assert model.target_date == date(2024, 1, 1)
    assert model.target_value == 100.0
    assert model.target_upper_bound == 110.0
    assert model.target_lower_bound == 90.0
    assert model.yellow_buffer == 5.0
    assert model.red_buffer == 10.0

    # Test with optional fields omitted
    model = MetricTarget(
        tenant_id=1,
        metric_id="test_metric",
        grain=Granularity.DAY,
        target_date=date(2024, 1, 1),
        target_value=100.0,
    )
    assert model.target_upper_bound is None
    assert model.target_lower_bound is None
    assert model.yellow_buffer is None
    assert model.red_buffer is None

    # Test invalid value type
    with pytest.raises(ValidationError):
        MetricTarget.model_validate(
            dict(
                tenant_id=1,
                metric_id="test_metric",
                grain=Granularity.DAY,
                target_date=date(2024, 1, 1),
                target_value="invalid",  # type: ignore
            )
        )

    # Test missing required fields
    with pytest.raises(ValidationError):
        MetricTarget.model_validate(
            dict(
                tenant_id=1,
                metric_id="test_metric",
                # Missing grain
                target_date=date(2024, 1, 1),
                target_value=100.0,
            )
        )

    # Test invalid grain value
    with pytest.raises(ValidationError):
        MetricTarget.model_validate(
            dict(
                tenant_id=1,
                metric_id="test_metric",
                grain="INVALID_GRAIN",  # type: ignore
                target_date=date(2024, 1, 1),
                target_value=100.0,
            )
        )

    # Test with invalid date type
    with pytest.raises(ValidationError):
        MetricTarget.model_validate(
            dict(
                tenant_id=1,
                metric_id="test_metric",
                grain=Granularity.DAY,
                target_date="not-a-date",  # type: ignore
                target_value=100.0,
            )
        )
