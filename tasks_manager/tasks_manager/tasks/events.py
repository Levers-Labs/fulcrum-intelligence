"""
Common event emission functions for tenant and metric sync operations.
"""

import logging
from datetime import datetime
from typing import Any

from prefect.events import emit_event

from commons.models.enums import Granularity
from query_manager.semantic_manager.models import SyncOperation
from tasks_manager.tasks.semantic_manager import SyncSummary

logger = logging.getLogger(__name__)


def send_tenant_sync_started_event(
    tenant_id: int,
    sync_operation: SyncOperation,
    grain: Granularity,
    **kwargs: Any,
) -> None:
    """Send event when tenant sync operation starts."""
    try:
        event_name = f"tenant.{sync_operation.value.lower()}.started"

        emit_event(
            event=event_name,
            resource={
                "prefect.resource.id": f"tenant.{tenant_id}",
                "tenant_id": str(tenant_id),
                "sync_operation": sync_operation.value,
                "grain": grain.value,
            },
            payload={
                "timestamp": datetime.now().isoformat(),
                **kwargs,
            },
        )
        logger.info(
            "Emitted tenant sync started event - Tenant: %d, Operation: %s, Grain: %s",
            tenant_id,
            sync_operation.value,
            grain.value,
        )
    except Exception as e:
        logger.error(
            "Failed to send tenant sync started event - Tenant: %d, Operation: %s, Error: %s",
            tenant_id,
            sync_operation.value,
            str(e),
        )


def send_tenant_sync_finished_event(
    tenant_id: int,
    sync_operation: SyncOperation,
    grain: Granularity,
    summary: SyncSummary,
    error: str | None = None,
    **kwargs: Any,
) -> None:
    """Send event when tenant sync operation finishes."""
    try:
        event_name = f"tenant.{sync_operation.value.lower()}.finished"

        emit_event(
            event=event_name,
            resource={
                "prefect.resource.id": f"tenant.{tenant_id}",
                "tenant_id": str(tenant_id),
                "sync_operation": sync_operation.value,
                "grain": grain.value,
            },
            payload={
                "summary": summary,
                "error": error,
                "timestamp": datetime.now().isoformat(),
                **kwargs,
            },
        )
        logger.info(
            "Emitted tenant sync finished event - Tenant: %d, Operation: %s, Grain: %s, Status: %s",
            tenant_id,
            sync_operation.value,
            grain.value,
            summary["status"],
        )
    except Exception as e:
        logger.error(
            "Failed to send tenant sync finished event - Tenant: %d, Operation: %s, Error: %s",
            tenant_id,
            sync_operation.value,
            str(e),
        )


def send_tenant_sync_requested_event(
    tenant_id: int,
    sync_operation: SyncOperation,
    grain: Granularity | None = None,
    **kwargs: Any,
) -> None:
    """Send event when tenant sync operation is requested."""
    try:
        event_name = f"tenant.{sync_operation.value.lower()}.requested"

        resource = {
            "prefect.resource.id": f"tenant.{tenant_id}",
            "tenant_id": str(tenant_id),
            "sync_operation": sync_operation.value,
        }

        if grain:
            resource["grain"] = grain.value

        emit_event(
            event=event_name,
            resource=resource,
            payload={
                "timestamp": datetime.now().isoformat(),
                **kwargs,
            },
        )
        logger.info(
            "Emitted tenant sync requested event - Tenant: %d, Operation: %s, Grain: %s",
            tenant_id,
            sync_operation.value,
            grain.value if grain else "ALL",
        )
    except Exception as e:
        logger.error(
            "Failed to send tenant sync requested event - Tenant: %d, Operation: %s, Error: %s",
            tenant_id,
            sync_operation.value,
            str(e),
        )
