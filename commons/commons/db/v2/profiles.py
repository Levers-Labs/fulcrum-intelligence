from __future__ import annotations

import os
from typing import Any

# Base options for all environments
_BASE: dict[str, Any] = {
    "pool_pre_ping": True,
    "pool_reset_on_return": "rollback",
    "pool_recycle": 180,  # 3min
    "echo": False,
    "future": True,
    "connect_args": {
        "command_timeout": 180,  # 3min
        "server_settings": {
            "application_name": os.getenv("APP_NAME", "fulcrum-intellegence"),
            "statement_timeout": "300000",  # 5min
            "idle_in_transaction_session_timeout": "300000",  # 5min idle timeout
            "lock_timeout": "30s",  # Lock timeout
        },
    },
}

# Default for dev/local environment
_DEV: dict[str, Any] = {
    "pool_size": 5,
    "max_overflow": 20,
    "pool_timeout": 30,
    "pool_recycle": 600,  # Override base - longer for dev
    "echo": True,
}

# Default for prod environment
_PROD: dict[str, Any] = {
    "pool_size": 12,
    "max_overflow": 18,
    "pool_timeout": 45,
}

# Heavy traffic environment
_LARGE: dict[str, Any] = {
    "pool_size": 20,
    "max_overflow": 30,
    "pool_timeout": 30,
}


# Short-lived tasks (Background jobs, ephemeral tasks)
_MICRO: dict[str, Any] = {
    "pool_size": 3,
    "max_overflow": 5,
    "pool_timeout": 30,
}


def _sanitize_for_unpooled(opts: dict[str, Any]) -> dict[str, Any]:
    poolclass = opts.get("poolclass")
    poolclass_name = poolclass if isinstance(poolclass, str) else getattr(poolclass, "__name__", "")
    if poolclass_name in {"NullPool", "StaticPool"}:
        # Remove args not supported by NullPool/StaticPool
        for k in (
            "pool_size",
            "max_overflow",
            "pool_timeout",
            "pool_recycle",
            "pool_pre_ping",
            "pool_reset_on_return",
        ):
            opts.pop(k, None)
    return opts


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = {**base}
    if "connect_args" in override:
        base_ca = dict(base.get("connect_args", {}))
        override_ca = override.get("connect_args", {}) or {}
        if "server_settings" in override_ca:
            base_ss = dict(base_ca.get("server_settings", {}))
            base_ss.update(override_ca.get("server_settings") or {})
            base_ca["server_settings"] = base_ss
        base_ca.update({k: v for k, v in override_ca.items() if k != "server_settings"})
        merged["connect_args"] = base_ca
    merged.update({k: v for k, v in override.items() if k != "connect_args"})
    return merged


def build_engine_options(
    profile: str | None, overrides: dict[str, Any] | None = None, app_name: str | None = None
) -> dict[str, Any]:
    """Build engine options from a named profile with optional deep-merge overrides.

    Supported profiles:
    - "prod": Standard production (Analysis Manager, Story Manager) - 12/18 pool
    - "dev": Development environment - 5/20 pool, verbose logging
    - "test": Test environment - StaticPool, no echo
    - "large": High-compute workloads (Query Manager, Insights Backend) - 20/30 pool
    - "micro": Low-resource workloads (Background jobs, workers) - 3/5 pool
    """
    opts = dict(_BASE)
    if profile == "prod":
        opts = _deep_merge(opts, _PROD)
    elif profile == "dev":
        opts = _deep_merge(opts, _DEV)
    elif profile == "test":
        opts = _deep_merge(opts, {"poolclass": "StaticPool", "echo": False})
    elif profile == "large":
        opts = _deep_merge(opts, _LARGE)
    elif profile == "micro":
        opts = _deep_merge(opts, _MICRO)

    if overrides:
        opts = _deep_merge(opts, overrides)

    # Allow explicit application name override
    if app_name:
        opts["connect_args"]["server_settings"]["application_name"] = app_name

    # strip incompatible pool args for unpooled configurations
    opts = _sanitize_for_unpooled(opts)
    return opts
