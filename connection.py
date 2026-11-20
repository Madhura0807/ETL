"""Engine creation from environment variables (no credentials in code or YAML)."""
from __future__ import annotations

import os
from urllib.parse import quote_plus

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.exceptions import ConfigError
from src.utils.retry import is_transient_db_error, retry_call


def build_target_url(settings: dict) -> str:
    env_name = settings.get("database", {}).get("url_env", "ETL_TARGET_URL")
    if os.environ.get(env_name):
        return os.environ[env_name]
    needed = ["MYSQL_HOST", "MYSQL_USER", "MYSQL_PASSWORD", "MYSQL_DATABASE"]
    missing = [k for k in needed if not os.environ.get(k)]
    if missing:
        raise ConfigError(f"target database not configured: set {env_name} or {', '.join(missing)}")
    port = os.environ.get("MYSQL_PORT", "3306")
    return (f"mysql+pymysql://{quote_plus(os.environ['MYSQL_USER'])}:{quote_plus(os.environ['MYSQL_PASSWORD'])}"
            f"@{os.environ['MYSQL_HOST']}:{port}/{os.environ['MYSQL_DATABASE']}?charset=utf8mb4")


def build_engine(settings: dict) -> Engine:
    return create_engine(build_target_url(settings), pool_pre_ping=True, future=True)


def check_connection(engine: Engine, retry_cfg: dict, logger=None, sleep=None) -> None:
    """Fail fast (after retries) if the target database is unreachable."""
    def ping():
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    kwargs = {"sleep": sleep} if sleep else {}
    retry_call(ping, max_attempts=retry_cfg["max_attempts"], backoff_seconds=retry_cfg["backoff_seconds"],
               backoff_factor=retry_cfg["backoff_factor"], should_retry=is_transient_db_error,
               description="database connection", logger=logger, **kwargs)
