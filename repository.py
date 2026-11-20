"""All reads/writes of pipeline metadata live here (keeps SQL out of the orchestrator)."""
from __future__ import annotations

import json

from sqlalchemy import delete, insert, select, update
from sqlalchemy.engine import Engine

from src.db import models as m
from src.utils.time import utcnow


class MetadataRepository:
    def __init__(self, engine: Engine):
        self.engine = engine

    def create_tables(self) -> None:
        m.metadata.create_all(self.engine)  # checkfirst: safe to call every run

    def _exec(self, stmt, params=None, conn=None):
        """Run ``stmt`` (optionally executemany with ``params``) in ``conn`` or its own transaction."""
        if conn is not None:
            return conn.execute(stmt, params) if params is not None else conn.execute(stmt)
        with self.engine.begin() as c:
            return c.execute(stmt, params) if params is not None else c.execute(stmt)

    # ----------------------------------------------------------- run history
    def start_run(self, execution_id, pipeline_name, source_type, start_time) -> None:
        self._exec(insert(m.etl_pipeline_runs).values(
            execution_id=execution_id, pipeline_name=pipeline_name, source_type=source_type,
            status="RUNNING", start_time=start_time))

    def finish_run(self, execution_id, report: dict) -> None:
        c = report["counts"]
        self._exec(update(m.etl_pipeline_runs).where(m.etl_pipeline_runs.c.execution_id == execution_id).values(
            status=report["status"], end_time=report["end_time_dt"], duration_seconds=report["duration_seconds"],
            extracted_count=c["extracted"], transformed_count=c["transformed"], valid_count=c["valid"],
            invalid_count=c["invalid"], loaded_count=c["loaded"], failed_batch_count=c["failed_batches"],
            schema_drift_status=report["schema_drift_status"], error_message=report.get("error"),
            report_json=json.dumps({k: v for k, v in report.items() if k != "end_time_dt"}, default=str)))

    # --------------------------------------------------------- quality results
    def save_quality_results(self, execution_id, results: list[dict]) -> None:
        if not results:
            return
        now = utcnow()
        self._exec(insert(m.etl_data_quality_results), [
            dict(execution_id=execution_id, check_name=r["check_name"][:160], level=r["level"], unit=r["unit"],
                 total_checked=r["total_checked"], passed=r["passed"], failed=r["failed"],
                 failure_pct=r["failure_pct"], status=r["status"],
                 error_details=json.dumps(r["error_details"]), created_at=now) for r in results])

    # ------------------------------------------------------------------ schema
    def save_schema_version(self, pipeline_name, version, execution_id, schema: dict) -> None:
        self._exec(insert(m.etl_schema_metadata).values(
            pipeline_name=pipeline_name, version=version, execution_id=execution_id,
            schema_json=json.dumps(schema), created_at=utcnow()))

    def latest_schema(self, pipeline_name) -> tuple[int, dict] | None:
        with self.engine.connect() as conn:
            row = conn.execute(select(m.etl_schema_metadata.c.version, m.etl_schema_metadata.c.schema_json)
                               .where(m.etl_schema_metadata.c.pipeline_name == pipeline_name)
                               .order_by(m.etl_schema_metadata.c.version.desc()).limit(1)).first()
        return (row[0], json.loads(row[1])) if row else None

    def save_drift(self, pipeline_name, execution_id, report: dict) -> None:
        self._exec(insert(m.etl_schema_drift_history).values(
            pipeline_name=pipeline_name, execution_id=execution_id, previous_version=report["previous_version"],
            status=report["status"], decision=report["decision"], report_json=json.dumps(report, default=str),
            created_at=utcnow()))

    # -------------------------------------------------------------- quarantine
    def save_quarantine(self, execution_id, pipeline_name, stage, rows: list[tuple[str, dict]]) -> None:
        """rows = [(reason, record_dict), ...]"""
        if not rows:
            return
        now = utcnow()
        self._exec(insert(m.etl_quarantined_records), [
            dict(execution_id=execution_id, pipeline_name=pipeline_name, stage=stage, reason=reason[:5000],
                 record_json=json.dumps(rec, default=str), created_at=now) for reason, rec in rows])

    # ------------------------------------------------------------------ batches
    def log_batch(self, execution_id, pipeline_name, batch_number, row_count, status, attempts,
                  error=None, conn=None) -> None:
        self._exec(insert(m.etl_batch_log).values(
            execution_id=execution_id, pipeline_name=pipeline_name, batch_number=batch_number,
            row_count=row_count, status=status, attempts=attempts, error_message=error,
            created_at=utcnow()), conn=conn)

    def set_batch_status(self, execution_id, from_status, to_status) -> None:
        t = m.etl_batch_log
        self._exec(update(t).where(t.c.execution_id == execution_id, t.c.status == from_status)
                   .values(status=to_status))

    # --------------------------------------------------------------- watermarks
    def get_watermark(self, pipeline_name) -> str | None:
        with self.engine.connect() as conn:
            row = conn.execute(select(m.etl_watermarks.c.watermark_value)
                               .where(m.etl_watermarks.c.pipeline_name == pipeline_name)).first()
        return row[0] if row else None

    def set_watermark(self, pipeline_name, column, value, execution_id) -> None:
        t = m.etl_watermarks
        with self.engine.begin() as conn:
            conn.execute(delete(t).where(t.c.pipeline_name == pipeline_name))
            conn.execute(insert(t).values(pipeline_name=pipeline_name, watermark_column=column,
                                          watermark_value=str(value), execution_id=execution_id,
                                          updated_at=utcnow()))
