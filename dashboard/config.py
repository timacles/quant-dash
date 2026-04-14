"""Configuration loading and database connection helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import tomllib


BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / "config.toml"
PULL_STATS_PATH = BASE_DIR / "pull_stats.py"


def load_config() -> dict[str, Any]:
    with CONFIG_PATH.open("rb") as handle:
        return tomllib.load(handle)


def resolve_database_config(config: dict[str, Any]) -> dict[str, Any]:
    environment = str(config.get("site", {}).get("environment", "dev")).strip() or "dev"
    database_config = config.get("database", {})
    db_config = database_config.get(environment)
    if not isinstance(db_config, dict):
        raise ValueError(f"Missing database config for environment '{environment}'")
    return db_config


def build_connection_kwargs(config: dict[str, Any]) -> dict[str, Any]:
    db_config = resolve_database_config(config)
    allowed_keys = ("dbname", "host", "port", "user", "password", "service")
    connect_kwargs = {key: value for key, value in db_config.items() if key in allowed_keys and value not in (None, "")}
    environment = str(config.get("site", {}).get("environment", "dev")).strip() or "dev"
    if not connect_kwargs:
        raise ValueError(f"Database config for environment '{environment}' is empty or invalid")
    return connect_kwargs
