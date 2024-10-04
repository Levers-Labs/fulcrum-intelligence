from contextvars import ContextVar

# Define a ContextVar to hold the tenant ID
tenant_id_ctx: ContextVar[int | None] = ContextVar("tenant_id", default=None)


def set_tenant_id(tenant_id: int | None) -> None:
    """
    Set the tenant_id in the current context.
    """
    tenant_id_ctx.set(tenant_id)


def get_tenant_id() -> int | None:
    """
    Get the tenant_id from the current context.
    """
    return tenant_id_ctx.get()


def reset_context() -> None:
    """Reset tenant in the current context to None."""
    tenant_id_ctx.set(None)
