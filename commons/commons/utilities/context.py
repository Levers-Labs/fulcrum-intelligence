import contextvars

# Define a ContextVar to hold the tenant ID
tenant_id_cvar_obj = contextvars.ContextVar("tenant_id", default=None)


def set_tenant_id(tenant_id):
    """
    Set the tenant_id in the current context.
    """
    tenant_id_cvar_obj.set(tenant_id)


def get_tenant_id():
    """
    Get the tenant_id from the current context.
    """
    return tenant_id_cvar_obj.get()
