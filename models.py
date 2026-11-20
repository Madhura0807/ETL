"""Pipeline METADATA tables (all prefixed ``etl_`` and kept apart from business tables)."""
from sqlalchemy import (BigInteger, Column, DateTime, Float, ForeignKey, Index, Integer, MetaData, String, Table,
                        Text, UniqueConstraint)
from sqlalchemy.dialects import mysql

metadata = MetaData()
BigText = Text().with_variant(mysql.LONGTEXT(), "mysql")
# SQLite only auto-increments INTEGER PRIMARY KEY; MySQL wants BIGINT.
BigPK = BigInteger().with_variant(Integer(), "sqlite")

etl_pipeline_runs = Table(
    "etl_pipeline_runs", metadata,
    Column("execution_id", String(36), primary_key=True),
    Column("pipeline_name", String(64), nullable=False),
    Column("source_type", String(16)),
    Column("status", String(24), nullable=False),
    Column("start_time", DateTime, nullable=False),
    Column("end_time", DateTime),
    Column("duration_seconds", Float),
    Column("extracted_count", Integer, nullable=False, default=0),
    Column("transformed_count", Integer, nullable=False, default=0),
    Column("valid_count", Integer, nullable=False, default=0),
    Column("invalid_count", Integer, nullable=False, default=0),
    Column("loaded_count", Integer, nullable=False, default=0),
    Column("failed_batch_count", Integer, nullable=False, default=0),
    Column("schema_drift_status", String(24)),
    Column("error_message", Text),
    Column("report_json", BigText),
    Index("ix_runs_pipeline_start", "pipeline_name", "start_time"),
    Index("ix_runs_status", "status"),
)

etl_data_quality_results = Table(
    "etl_data_quality_results", metadata,
    Column("id", BigPK, primary_key=True, autoincrement=True),
    Column("execution_id", String(36), ForeignKey("etl_pipeline_runs.execution_id"), nullable=False),
    Column("check_name", String(160), nullable=False),
    Column("level", String(10), nullable=False),
    Column("unit", String(16), nullable=False),
    Column("total_checked", Integer, nullable=False),
    Column("passed", Integer, nullable=False),
    Column("failed", Integer, nullable=False),
    Column("failure_pct", Float, nullable=False),
    Column("status", String(10), nullable=False),
    Column("error_details", Text),
    Column("created_at", DateTime, nullable=False),
    Index("ix_dq_execution", "execution_id"),
    Index("ix_dq_check_status", "check_name", "status"),
)

etl_schema_metadata = Table(
    "etl_schema_metadata", metadata,
    Column("id", BigPK, primary_key=True, autoincrement=True),
    Column("pipeline_name", String(64), nullable=False),
    Column("version", Integer, nullable=False),
    Column("execution_id", String(36), nullable=False),
    Column("schema_json", BigText, nullable=False),
    Column("created_at", DateTime, nullable=False),
    UniqueConstraint("pipeline_name", "version", name="uq_schema_pipeline_version"),
)

etl_schema_drift_history = Table(
    "etl_schema_drift_history", metadata,
    Column("id", BigPK, primary_key=True, autoincrement=True),
    Column("pipeline_name", String(64), nullable=False),
    Column("execution_id", String(36), nullable=False),
    Column("previous_version", Integer),
    Column("status", String(24), nullable=False),
    Column("decision", String(10), nullable=False),
    Column("report_json", BigText, nullable=False),
    Column("created_at", DateTime, nullable=False),
    Index("ix_drift_pipeline_created", "pipeline_name", "created_at"),
)

etl_quarantined_records = Table(
    "etl_quarantined_records", metadata,
    Column("id", BigPK, primary_key=True, autoincrement=True),
    Column("execution_id", String(36), ForeignKey("etl_pipeline_runs.execution_id"), nullable=False),
    Column("pipeline_name", String(64), nullable=False),
    Column("stage", String(16), nullable=False),     # extraction | validation | load
    Column("reason", Text, nullable=False),
    Column("record_json", BigText, nullable=False),
    Column("created_at", DateTime, nullable=False),
    Index("ix_quarantine_execution", "execution_id"),
    Index("ix_quarantine_pipeline_stage", "pipeline_name", "stage"),
)

etl_batch_log = Table(
    "etl_batch_log", metadata,
    Column("id", BigPK, primary_key=True, autoincrement=True),
    Column("execution_id", String(36), ForeignKey("etl_pipeline_runs.execution_id"), nullable=False),
    Column("pipeline_name", String(64), nullable=False),
    Column("batch_number", Integer, nullable=False),
    Column("row_count", Integer, nullable=False),
    Column("status", String(16), nullable=False),    # SUCCESS | FAILED | STAGED | ROLLED_BACK
    Column("attempts", Integer, nullable=False, default=1),
    Column("error_message", Text),
    Column("created_at", DateTime, nullable=False),
    UniqueConstraint("execution_id", "batch_number", name="uq_batch_execution_number"),
)

etl_watermarks = Table(
    "etl_watermarks", metadata,
    Column("pipeline_name", String(64), primary_key=True),
    Column("watermark_column", String(64), nullable=False),
    Column("watermark_value", String(64), nullable=False),
    Column("execution_id", String(36), nullable=False),
    Column("updated_at", DateTime, nullable=False),
)
