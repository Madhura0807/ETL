"""Batch loader for MySQL (SQLite is supported as a drop-in for local tests/demos).

Guarantees
----------
* append / upsert : every batch is its own transaction. A failing batch rolls back ONLY itself;
  earlier batches stay committed. Failed rows are returned so they can be quarantined.
* full           : all-or-nothing. Rows are staged in a temporary table; only if EVERY batch
  staged successfully is the target replaced inside ONE transaction (DELETE + INSERT..SELECT).
  Otherwise the staging table is dropped and the existing target data is untouched.
* retries are safe: a retried batch is a fresh transaction, and append(skip)/upsert statements
  are idempotent on the primary key.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field

import pandas as pd
from pandas.api import types as pdt
from sqlalchemy import (BigInteger, Boolean, Column, Date, DateTime, Float, MetaData, String, Table, Text, delete,
                        insert, inspect, select, text)
from sqlalchemy.engine import Engine

from src.exceptions import LoadError, SchemaMismatchError
from src.utils.retry import is_transient_db_error, retry_call


@dataclass
class BatchResult:
    batch_number: int
    row_count: int
    status: str
    attempts: int
    error: str | None = None

    def to_dict(self) -> dict:
        return self.__dict__.copy()


@dataclass
class LoadResult:
    mode: str
    table: str
    rows_attempted: int
    loaded_count: int
    batches: list[BatchResult] = field(default_factory=list)
    failed_rows: pd.DataFrame = field(default_factory=pd.DataFrame)  # has '_load_error'
    aborted: bool = False   # full load that was rolled back

    @property
    def failed_batch_count(self) -> int:
        return sum(1 for b in self.batches if b.status == "FAILED")

    @property
    def successful_batch_count(self) -> int:
        return sum(1 for b in self.batches if b.status == "SUCCESS")


def sql_type_for(series: pd.Series, as_date: bool = False):
    if pdt.is_bool_dtype(series):
        return Boolean()
    if pdt.is_integer_dtype(series):
        return BigInteger()
    if pdt.is_float_dtype(series):
        return Float()
    if pdt.is_datetime64_any_dtype(series):
        return Date() if as_date else DateTime()
    longest = series.dropna().map(lambda v: len(str(v))).max() if series.notna().any() else 0
    return Text() if longest > 255 else String(255)


def to_records(df: pd.DataFrame, table: Table) -> list[dict]:
    """DataFrame -> list of plain-Python dicts (NaN/NaT/NA -> None, numpy -> native)."""
    obj = df.astype(object).where(df.notna(), None)
    records = obj.to_dict("records")
    date_cols = {c.name for c in table.c if getattr(c.type, "python_type", None) is __import__("datetime").date}
    for rec in records:
        for k, v in rec.items():
            if isinstance(v, pd.Timestamp):
                v = v.to_pydatetime()
                rec[k] = v.date() if k in date_cols else v
            elif hasattr(v, "item") and not isinstance(v, (str, bytes)):
                rec[k] = v.item()
    return records


class MySQLLoader:
    def __init__(self, engine: Engine, table: str, mode: str, batch_size: int, primary_key: list[str],
                 on_duplicate: str, retry_cfg: dict, repository=None, pipeline_name: str = "",
                 date_columns=(), logger: logging.Logger | None = None, sleep=time.sleep):
        if engine.dialect.name not in {"mysql", "sqlite"}:
            raise LoadError(f"unsupported database dialect: {engine.dialect.name}")
        self.engine, self.table_name, self.mode = engine, table, mode
        self.batch_size, self.pk, self.on_duplicate = batch_size, list(primary_key), on_duplicate
        self.retry, self.repo, self.pipeline = retry_cfg, repository, pipeline_name
        self.date_columns = set(date_columns)
        self.log = logger or logging.getLogger("etl.load")
        self.sleep = sleep

    # ------------------------------------------------------------- table prep
    def prepare_table(self, df: pd.DataFrame, add_columns=()) -> Table:
        """Create the business table if missing; otherwise verify (never silently ignore) columns."""
        insp = inspect(self.engine)
        if not insp.has_table(self.table_name):
            cols = [Column(c, sql_type_for(df[c], c in self.date_columns), primary_key=c in self.pk,
                           nullable=c not in self.pk) for c in df.columns]
            table = Table(self.table_name, MetaData(), *cols)
            table.create(self.engine)
            self.log.info("Created target table %s (%d columns)", self.table_name, len(cols))
            return Table(self.table_name, MetaData(), autoload_with=self.engine)

        table = Table(self.table_name, MetaData(), autoload_with=self.engine)
        existing = {c.name for c in table.c}
        new_cols = [c for c in df.columns if c not in existing]
        unexpected = [c for c in new_cols if c not in add_columns]
        if unexpected:
            raise SchemaMismatchError(
                f"columns {unexpected} do not exist in target table '{self.table_name}' and adding them "
                "was not approved by the schema-drift policy")
        for c in new_cols:
            ddl_type = sql_type_for(df[c], c in self.date_columns).compile(dialect=self.engine.dialect)
            prep = self.engine.dialect.identifier_preparer
            with self.engine.begin() as conn:
                conn.execute(text(f"ALTER TABLE {prep.quote(self.table_name)} ADD COLUMN {prep.quote(c)} {ddl_type} NULL"))
            self.log.warning("Added column %s.%s (%s) per schema-drift policy", self.table_name, c, ddl_type)
        if new_cols:
            table = Table(self.table_name, MetaData(), autoload_with=self.engine)
        if self.pk and (self.mode == "upsert" or self.on_duplicate in {"skip", "update"}):
            target_pk = {c.name for c in table.primary_key.columns}
            if target_pk != set(self.pk):
                raise SchemaMismatchError(
                    f"target table '{self.table_name}' primary key {sorted(target_pk)} differs from configured "
                    f"{sorted(self.pk)}; duplicate handling would not be reliable")
        return table

    # ----------------------------------------------------------- statements
    def _insert_stmt(self, table: Table, columns: list[str], plain: bool = False):
        dialect = self.engine.dialect.name
        if dialect == "mysql":
            from sqlalchemy.dialects.mysql import insert as dinsert
        else:
            from sqlalchemy.dialects.sqlite import insert as dinsert
        stmt = dinsert(table)
        if plain or not self.pk:
            return stmt
        update_cols = [c for c in columns if c not in self.pk]
        wants_update = self.mode == "upsert" or self.on_duplicate == "update"
        wants_skip = self.mode == "append" and self.on_duplicate == "skip"
        if wants_update and update_cols:
            if dialect == "mysql":
                return stmt.on_duplicate_key_update({c: stmt.inserted[c] for c in update_cols})
            return stmt.on_conflict_do_update(index_elements=self.pk, set_={c: stmt.excluded[c] for c in update_cols})
        if wants_update or wants_skip:
            if dialect == "mysql":
                # no-op update instead of INSERT IGNORE (which would also hide NOT NULL/truncation errors)
                return stmt.on_duplicate_key_update({self.pk[0]: table.c[self.pk[0]]})
            return stmt.on_conflict_do_nothing(index_elements=self.pk)
        return stmt  # on_duplicate == error -> plain insert, duplicates raise

    def _execute_batch(self, table: Table, stmt, records: list[dict], batch_number: int,
                       execution_id: str, status: str) -> None:
        """One transaction: data + batch-log row commit (or roll back) together."""
        with self.engine.begin() as conn:
            conn.execute(stmt, records)
            if self.repo:
                self.repo.log_batch(execution_id, self.pipeline, batch_number, len(records), status,
                                    self._attempts, conn=conn)

    def _run_batches(self, df: pd.DataFrame, table: Table, stmt, execution_id: str, success_status: str):
        batches: list[BatchResult] = []
        failed_frames: list[pd.DataFrame] = []
        columns = list(df.columns)
        for n, start in enumerate(range(0, len(df), self.batch_size), start=1):
            chunk = df.iloc[start:start + self.batch_size]
            records = to_records(chunk, table)
            self._attempts = 0

            def attempt():
                self._attempts += 1
                self._execute_batch(table, stmt, records, n, execution_id, success_status)

            try:
                retry_call(attempt, max_attempts=self.retry["max_attempts"],
                           backoff_seconds=self.retry["backoff_seconds"], backoff_factor=self.retry["backoff_factor"],
                           should_retry=is_transient_db_error, description=f"load batch {n}",
                           logger=self.log, sleep=self.sleep)
                batches.append(BatchResult(n, len(chunk), success_status, self._attempts))
                self.log.info("Batch %d OK (%d rows, %d attempt(s))", n, len(chunk), self._attempts)
            except Exception as exc:  # noqa: BLE001 - a bad batch must not stop the others
                err = f"{exc.__class__.__name__}: {str(exc)[:500]}"
                self.log.error("Batch %d FAILED after %d attempt(s): %s", n, self._attempts, err)
                batches.append(BatchResult(n, len(chunk), "FAILED", self._attempts, err))
                bad = chunk.copy()
                bad["_load_error"] = f"batch {n}: {err}"
                failed_frames.append(bad)
                if self.repo:
                    try:
                        self.repo.log_batch(execution_id, self.pipeline, n, len(chunk), "FAILED",
                                            self._attempts, err)
                    except Exception:  # noqa: BLE001
                        self.log.exception("Could not record failed batch %d in etl_batch_log", n)
        failed = pd.concat(failed_frames) if failed_frames else pd.DataFrame(columns=columns + ["_load_error"])
        return batches, failed

    # ----------------------------------------------------------------- public
    def load(self, df: pd.DataFrame, execution_id: str, add_columns=()) -> LoadResult:
        table = self.prepare_table(df, add_columns)
        if df.empty:
            return LoadResult(self.mode, self.table_name, 0, 0)
        if self.mode == "full":
            return self._load_full(df, table, execution_id)
        stmt = self._insert_stmt(table, list(df.columns))
        batches, failed = self._run_batches(df, table, stmt, execution_id, "SUCCESS")
        loaded = sum(b.row_count for b in batches if b.status == "SUCCESS")
        return LoadResult(self.mode, self.table_name, len(df), loaded, batches, failed)

    def _load_full(self, df: pd.DataFrame, table: Table, execution_id: str) -> LoadResult:
        staging_name = f"{self.table_name}__stg_{execution_id.replace('-', '')[:8]}"
        staging = Table(staging_name, MetaData(),
                        *[Column(c.name, c.type, primary_key=c.primary_key, nullable=c.nullable) for c in table.c])
        staging.create(self.engine)
        try:
            stmt = self._insert_stmt(staging, list(df.columns), plain=True)
            batches, failed = self._run_batches(df, staging, stmt, execution_id, "STAGED")
            if failed.shape[0]:
                self.log.error("Full load aborted: %d batch(es) failed; target table left untouched",
                               sum(1 for b in batches if b.status == "FAILED"))
                if self.repo:
                    self.repo.set_batch_status(execution_id, "STAGED", "ROLLED_BACK")
                return LoadResult(self.mode, self.table_name, len(df), 0, batches, failed, aborted=True)
            cols = list(df.columns)

            def swap():
                with self.engine.begin() as conn:
                    conn.execute(delete(table))
                    conn.execute(insert(table).from_select(cols, select(*[staging.c[c] for c in cols])))

            retry_call(swap, max_attempts=self.retry["max_attempts"], backoff_seconds=self.retry["backoff_seconds"],
                       backoff_factor=self.retry["backoff_factor"], should_retry=is_transient_db_error,
                       description="full-load swap", logger=self.log, sleep=self.sleep)
            if self.repo:
                self.repo.set_batch_status(execution_id, "STAGED", "SUCCESS")
            for b in batches:
                b.status = "SUCCESS"
            return LoadResult(self.mode, self.table_name, len(df), len(df), batches, failed)
        except Exception as exc:  # swap failed: transaction rolled back, old data intact
            self.log.error("Full-load swap failed, target untouched: %s", exc)
            if self.repo:
                self.repo.set_batch_status(execution_id, "STAGED", "ROLLED_BACK")
            raise LoadError(f"full load swap failed (target table unchanged): {exc}") from exc
        finally:
            staging.drop(self.engine, checkfirst=True)
