# Fulcrum Intelligence Engine – Caching Guide

## Overview
This document explains the shared caching layer implemented in `commons.cache`, how each FastAPI micro-service integrates with it, and operational guidelines for running it in development, staging, and production.

---

## Installation

1. **Install dependency** (top-level project):
   ```bash
   poetry add "fastapi-cache2[redis]" -C commons  # added in commons/pyproject.toml
   ```
   The package is declared in `commons/pyproject.toml`, so all services that depend on `commons` get the library transitively.

---

## 1. Directory Layout
```
commons/commons/cache/
├── __init__.py      # Public API (import from here)
├── cache.py         # Backend selection – Redis with automatic fallback to InMemory
├── keys.py          # Multi-tenant and user-aware cache-key builders
└── settings.py      # Typed settings loader (pydantic-settings)
```

---

## 2. Key Components

| File | Responsibility |
|------|----------------|
| **settings.py** | Loads env vars → `CacheSettings` (singleton via `@lru_cache`). |
| **cache.py**    | `init_cache()` decides between Redis and InMemory; `close_cache()` closes connections. |
| **keys.py**     | `tenant_cache_key_builder` (tenant-scoped) and placeholder `tenant_user_cache_key_builder` (user-scoped). |
| **__init__.py** | Re-exports helpers and provides `invalidate_namespace()` for easy invalidation. |

---

## 3. Environment Variables (per service)

| Variable | Example | Description |
|----------|---------|-------------|
| `CACHE_ENABLED` | `true` (default) | Disable caching globally if set to `false`. |
| `CACHE_PREFIX`  | `ib-cache`      | Service-specific prefix prepended to every Redis key. Ensures isolation between micro-services that share the same Redis instance. |
| `CACHE_TTL_SECONDS` | `300` | Default TTL used by routes (can be overridden per-route via `@cache(expire=…)`). |
| `REDIS_URL`     | `redis://redis:6379` | Connection string for Redis. Omit in dev to stick with In-Memory backend. |

> **Tip**  Give each service a unique prefix (`ib-cache`, `am-cache`, `sm-cache`, …) so you can bulk-delete service keys with `redis-cli KEYS ib-cache:*`.

---

## 4. Service Integration Steps

1. **Add env vars**
   ```dotenv
   # insights_backend/.env
   CACHE_PREFIX=ib-cache
   CACHE_TTL_SECONDS=300
   REDIS_URL=redis://redis:6379   # optional in dev
   ```

2. **Wire lifespan**
   ```python
   from contextlib import asynccontextmanager
   from commons.cache import init_cache, close_cache

   @asynccontextmanager
   async def lifespan(app):
       await init_cache()         # picks Redis or InMemory automatically
       try:
           yield
       finally:
           await close_cache()
   ```

3. **Cache a route**
   ```python
   from fastapi_cache.decorator import cache
   from commons.cache import tenant_cache_key_builder, cache_settings

   @router.get("/tenant/config")
   @cache(
       expire=cache_settings().CACHE_TTL_SECONDS,
       namespace="tenant_config",
       key_builder=tenant_cache_key_builder,
   )
   async def get_tenant_config(...):
       ...
   ```

4. **Invalidate after mutations**
   ```python
   from commons.cache import invalidate_namespace

   await invalidate_namespace("tenant_config")
   ```

---

## 5. Cache-Key Strategy

### 5.1 Tenant-Scoped (default)
```
{CACHE_PREFIX}:{namespace}:tenant_{tenant_id}:{url_path}:{md5(query_params)}
```
Ensures:
* No cross-tenant data leakage.
* Different query params (pagination, filters) create separate entries.

### 5.2 User-Scoped (future)

`keys.py` contains a stub `tenant_user_cache_key_builder`.
Use it only for endpoints where the response is **user-specific** (e.g. `/me/profile`).

---

## 6. Invalidating Cache

1. **TTL Expiry** – handled automatically by `expire=` parameter.
2. **Manual Namespace Flush** – `await invalidate_namespace("tenant_config")`.
3. **Service-wide Flush** – `redis-cli KEYS ib-cache:* | xargs redis-cli DEL`.

---

## 7. Redis Fallback Logic

1. If `REDIS_URL` is set and `redis.ping()` succeeds → RedisBackend.
2. Otherwise logs a warning and falls back to InMemoryBackend.
3. Both branches share the same public API; no changes to route code.

---

## 8. Observability & Debugging

* `X-FastAPI-Cache` response header: `HIT` or `MISS`.
* Enable DEBUG logging (`LOGGING_LEVEL=DEBUG`) to see per-request HIT/MISS logs.
* In Redis: keys start with `CACHE_PREFIX`; monitor `Keyspace Hits`, `Misses`, `Evictions` in CloudWatch.

---

## 9. Operational Tips

| Environment | Backend | Notes |
|-------------|---------|-------|
| **Local / CI** | InMemoryBackend | No external infra; resets per run. |
| **Staging / Prod** | RedisBackend (ElastiCache) | Multi-AZ, TLS, `maxmemory-policy allkeys-lru`. |

**Fail-fast vs graceful degradation**
* Current code logs error & falls back. Change behaviour by raising the exception in `cache.py` if we want the service to refuse start-up when Redis is unavailable.

---

Happy caching! 🚀
