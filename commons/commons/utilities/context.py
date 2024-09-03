# Define a ContextVar to hold the tenant ID
TENANT_ID = ""


def set_tenant_id(tenant_id: str):
    global TENANT_ID
    TENANT_ID = tenant_id


def get_tenant_id() -> str:
    return TENANT_ID
