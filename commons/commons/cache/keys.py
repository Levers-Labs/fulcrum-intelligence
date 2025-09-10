import hashlib

from fastapi import Request

from commons.utilities.context import get_tenant_id


def _hash_qp(request: Request) -> str:
    """Hash query parameters for shorter cache keys."""
    return hashlib.sha256(repr(sorted(request.query_params.items())).encode()).hexdigest()


def tenant_cache_key_builder(func, namespace: str = "", *, request: Request, **kwargs):
    """
    Cache key builder that includes tenant context for multi-tenant isolation.

    Args:
        func: The function to be cached.
        namespace: The namespace for the cache.
        request: The request object.
        kwargs: Additional keyword arguments.

    Returns:
        The cache key.
    """
    tenant_id = get_tenant_id()
    if tenant_id is None:
        return f"{namespace}:{request.url.path}:{_hash_qp(request)}"

    return f"{namespace}:tenant_{tenant_id}:{request.url.path}:{_hash_qp(request)}"


def tenant_user_cache_key_builder(func, namespace: str = "", *, request: Request, **kwargs):
    """
    Cache key builder that includes both tenant and user context.

    """
    # TODO: Discuss if needed, then implement
    pass
