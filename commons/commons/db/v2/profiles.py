from __future__ import annotations

import os
from typing import Any

_BASE: dict[str, Any] = {
    "pool_pre_ping": True,
    "pool_reset_on_return": "rollback",
    "echo": False,
    "future": True,
}


_DEV: dict[str, Any] = {
    "pool_size": 5,
    "max_overflow": 20,
    "pool_timeout": 30,
    "pool_recycle": 600,
    "echo": True,
}


_PROD: dict[str, Any] = {
    "pool_size": 25,
    "max_overflow": 40,
    "pool_timeout": 45,
    "pool_recycle": 1200,
    "connect_args": {
        "command_timeout": 180,
        "server_settings": {
            "application_name": os.getenv("APP_NAME", "fulcrum-intellegence"),
            "statement_timeout": "600000",
            "idle_in_transaction_session_timeout": "900000",
        },
    },
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

    Supported profiles: "prod", "dev", "test"
    """
    opts = dict(_BASE)
    if profile == "prod":
        opts = _deep_merge(opts, _PROD)
    elif profile == "dev":
        opts = _deep_merge(opts, _DEV)
    elif profile == "test":
        opts = _deep_merge(opts, {"poolclass": "StaticPool", "echo": False})

    if overrides:
        opts = _deep_merge(opts, overrides)

    # Allow explicit application name override
    if app_name:
        ca = dict(opts.get("connect_args", {}))
        ss = dict(ca.get("server_settings", {}))
        ss["application_name"] = app_name
        ca["server_settings"] = ss
        opts["connect_args"] = ca

    # strip incompatible pool args for unpooled configurations
    opts = _sanitize_for_unpooled(opts)
    return opts
