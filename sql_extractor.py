from __future__ import annotations

import os
import re

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy import exc as sa_exc

from src.exceptions import ExtractionError
from src.extractors.base import BaseExtractor
from src.utils.retry import is_transient_db_error, retry_call


class SQLExtractor(BaseExtractor):
    """Runs one configured SELECT against a source database.

    The connection URL is read from the environment variable named in ``connection_env``
    so credentials are never stored in YAML. Query parameters use bound ``:name`` placeholders.
    """

    source_type = "sql"

    def _extract(self):
        env_name = self.config["connection_env"]
        url = os.environ.get(env_name)
        if not url:
            raise ExtractionError(f"environment variable '{env_name}' (source DB URL) is not set")
        query = self.config["query"].strip().rstrip(";").strip()
        if not re.match(r"(?is)^(select|with)\b", query) or ";" in query:
            raise ExtractionError("only a single SELECT statement is allowed for SQL extraction")
        engine = create_engine(url, pool_pre_ping=True)

        def run():
            with engine.connect() as conn:
                return pd.read_sql_query(text(query), conn, params=self.config.get("params") or None)

        try:
            df = retry_call(
                run, max_attempts=self.retry["max_attempts"], backoff_seconds=self.retry["backoff_seconds"],
                backoff_factor=self.retry["backoff_factor"], should_retry=is_transient_db_error,
                description="source SQL query", logger=self.log, **({"sleep": self._sleep} if self._sleep else {}),
            )
        except sa_exc.SQLAlchemyError as exc:
            raise ExtractionError(f"source database query failed: {exc.__class__.__name__}: {exc}") from exc
        finally:
            engine.dispose()
        return df, [], {"connection_env": env_name, "query": query}
