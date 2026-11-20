"""End-to-end orchestrator tests (SQLite stands in for MySQL; see docs for the dialect note)."""
import json
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from src.config_loader import validate_pipeline_config
from src.pipeline.orchestrator import PipelineOrchestrator

NOSLEEP = lambda s: None  # noqa: E731
CSV = "id,Name,Email\n1,Ann,ann@x.com\n2,Bob,bob@x.com\n3,Cy,cy@x.com\n4,Di,not-an-email\n"


@pytest.fixture
def csv_file(tmp_path):
    p = tmp_path / "people.csv"
    p.write_text(CSV)
    return p


@pytest.fixture
def cfg(make_config, csv_file):
    def _cfg(**sections):
        base = dict(
            source={"type": "csv", "path": str(csv_file)},
            transformations={"standardize_columns": True, "trim_strings": True, "type_conversions": {"id": "integer"}},
            quality_checks={"primary_key": "id", "required_columns": ["id", "name"], "email_columns": ["email"]},
            load={"mode": "upsert", "batch_size": 2},
        )
        base.update(sections)
        return make_config(**base)
    return _cfg


def run(config, settings, engine, **kw):
    return PipelineOrchestrator(config, settings, engine=engine, sleep=NOSLEEP, **kw).run()


def scalar(engine, sql):
    with engine.connect() as c:
        return c.execute(text(sql)).scalar()


def test_successful_run_end_to_end(cfg, settings, engine):
    rep = run(cfg(), settings, engine)
    assert rep["status"] == "SUCCESS"
    assert rep["counts"] == {"extracted": 4, "transformed": 4, "valid": 3, "invalid": 1, "loaded": 3, "failed_batches": 0}
    assert rep["schema_drift_status"] == "BASELINE_CREATED" and rep["schema_version"] == 1
    assert scalar(engine, "SELECT COUNT(*) FROM people") == 3
    # report contains every required field and was written to disk
    for key in ("pipeline_name", "execution_id", "start_time", "end_time", "duration_seconds", "source_type",
                "counts", "schema_drift_status", "status"):
        assert key in rep
    assert json.loads(Path(rep["artifacts"]["report_file"]).read_text())["status"] == "SUCCESS"
    assert Path(rep["artifacts"]["log_file"]).exists() and Path(rep["artifacts"]["processed_file"]).exists()


def test_metadata_tables_are_populated(cfg, settings, engine):
    rep = run(cfg(), settings, engine)
    eid = rep["execution_id"]
    assert scalar(engine, f"SELECT status FROM etl_pipeline_runs WHERE execution_id='{eid}'") == "SUCCESS"
    assert scalar(engine, f"SELECT loaded_count FROM etl_pipeline_runs WHERE execution_id='{eid}'") == 3
    assert scalar(engine, f"SELECT COUNT(*) FROM etl_data_quality_results WHERE execution_id='{eid}'") >= 3
    assert scalar(engine, f"SELECT COUNT(*) FROM etl_quarantined_records WHERE execution_id='{eid}' AND stage='validation'") == 1
    assert scalar(engine, "SELECT COUNT(*) FROM etl_schema_metadata WHERE pipeline_name='test_pipeline'") == 1
    assert scalar(engine, f"SELECT COUNT(*) FROM etl_batch_log WHERE execution_id='{eid}'") == 2
    q = Path(rep["artifacts"]["validation_quarantine"]).read_text()
    assert "not-an-email" in q and "invalid email" in q


def test_rerun_is_idempotent(cfg, settings, engine):
    run(cfg(), settings, engine)
    rep2 = run(cfg(), settings, engine)
    assert rep2["status"] == "SUCCESS" and rep2["schema_drift_status"] == "NONE"
    assert scalar(engine, "SELECT COUNT(*) FROM people") == 3
    assert scalar(engine, "SELECT COUNT(*) FROM etl_schema_metadata") == 1     # no new version without change


def test_removed_column_blocks_load_and_keeps_baseline(cfg, settings, engine, csv_file):
    run(cfg(), settings, engine)
    csv_file.write_text("id,Name\n10,Zed\n")
    rep = run(cfg(), settings, engine)
    assert rep["status"] == "BLOCKED" and rep["schema_drift_status"] == "CRITICAL"
    assert "email" in rep["error"].lower() or "Email" in rep["error"]
    assert scalar(engine, "SELECT COUNT(*) FROM people") == 3                   # nothing new loaded
    assert scalar(engine, "SELECT COUNT(*) FROM etl_schema_metadata") == 1      # baseline untouched
    assert scalar(engine, "SELECT COUNT(*) FROM etl_schema_drift_history") == 1
    assert Path(rep["artifacts"]["drift_report"]).exists()


def test_accept_drift_promotes_new_baseline(cfg, settings, engine, csv_file):
    run(cfg(), settings, engine)
    csv_file.write_text("id,Name\n10,Zed\n")
    no_email = {"primary_key": "id", "required_columns": ["id", "name"]}   # config no longer references email
    rep = run(cfg(quality_checks=no_email), settings, engine, accept_drift=True)
    assert rep["status"] == "SUCCESS" and rep["schema_version"] == 2
    assert scalar(engine, "SELECT COUNT(*) FROM people") == 4


def test_new_column_is_excluded_by_default_and_reported_every_run(cfg, settings, engine, csv_file):
    run(cfg(), settings, engine)
    csv_file.write_text("id,Name,Email,Phone\n1,Ann,ann@x.com,555\n")
    rep = run(cfg(), settings, engine)
    assert rep["status"] == "SUCCESS" and rep["schema_drift_status"] == "WARNING"
    assert "phone" not in [r[1] for r in engine.connect().execute(text("PRAGMA table_info(people)"))]
    assert any("excluded" in w for w in rep["warnings"])
    rep2 = run(cfg(), settings, engine)                                       # still flagged, still safe
    assert rep2["status"] == "SUCCESS" and rep2["schema_drift_status"] == "WARNING"
    assert scalar(engine, "SELECT COUNT(*) FROM etl_schema_metadata") == 1


def test_new_column_added_when_policy_is_add(cfg, settings, engine, csv_file):
    run(cfg(), settings, engine)
    csv_file.write_text("id,Name,Email,Phone\n1,Ann,ann@x.com,555\n")
    rep = run(cfg(schema_drift={"policies": {"column_added": "add"}}), settings, engine)
    assert rep["status"] == "SUCCESS" and rep["schema_version"] == 2
    assert scalar(engine, "SELECT phone FROM people WHERE id = 1") == "555"


def test_quality_gate_blocks_when_required_column_missing(cfg, settings, engine, csv_file):
    csv_file.write_text("id,Email\n1,a@x.com\n")
    rep = run(cfg(), settings, engine)
    assert rep["status"] == "BLOCKED" and "required_columns_present" in rep["error"]
    assert rep["counts"]["loaded"] == 0


def test_null_threshold_gate_blocks(cfg, settings, engine, csv_file):
    csv_file.write_text("id,Name,Email\n1,Ann,\n2,Bob,\n3,Cy,c@x.com\n")
    rep = run(cfg(quality_checks={"primary_key": "id", "null_threshold": 0.2, "null_threshold_columns": ["email"]}),
              settings, engine)
    assert rep["status"] == "BLOCKED" and rep["counts"]["loaded"] == 0


def test_empty_source_succeeds_without_touching_target(cfg, settings, engine, csv_file):
    run(cfg(), settings, engine)
    csv_file.write_text("id,Name,Email\n")
    rep = run(cfg(load={"mode": "full", "batch_size": 2}), settings, engine)
    assert rep["status"] == "SUCCESS" and rep["counts"]["extracted"] == 0
    assert scalar(engine, "SELECT COUNT(*) FROM people") == 3                  # a full load did NOT wipe the table


def test_missing_source_file_fails_cleanly_and_still_reports(cfg, settings, engine, tmp_path):
    rep = run(cfg(source={"type": "csv", "path": str(tmp_path / "nope.csv")}), settings, engine)
    assert rep["status"] == "FAILED" and "not found" in rep["error"]
    assert scalar(engine, f"SELECT status FROM etl_pipeline_runs WHERE execution_id='{rep['execution_id']}'") == "FAILED"
    assert Path(rep["artifacts"]["report_file"]).exists()


def test_database_unreachable_fails_gracefully_with_report(cfg, settings, tmp_path):
    bad = create_engine(f"sqlite:///{tmp_path}/no_such_dir/x.db")
    rep = run(cfg(), settings, bad)
    assert rep["status"] == "FAILED" and rep["counts"]["loaded"] == 0
    assert Path(rep["artifacts"]["report_file"]).exists()


def test_malformed_csv_rows_are_quarantined_at_extraction(cfg, settings, engine, csv_file):
    csv_file.write_text(CSV + "5,Eve,eve@x.com,EXTRA\n")
    rep = run(cfg(), settings, engine)
    assert rep["status"] == "SUCCESS" and rep["counts"]["invalid"] == 2           # 1 validation + 1 extraction
    assert "extraction_quarantine" in rep["artifacts"]


def test_partial_load_status_and_failed_rows_quarantined(cfg, settings, engine, csv_file):
    with engine.begin() as c:
        c.execute(text("CREATE TABLE people (id BIGINT PRIMARY KEY, name TEXT, email TEXT CHECK (email <> 'bob@x.com'))"))
    rep = run(cfg(), settings, engine)                    # batch_size 2: batch1=[ann,bob] fails, batch2=[cy] ok
    assert rep["status"] == "PARTIAL_SUCCESS"
    assert rep["counts"]["failed_batches"] == 1 and rep["counts"]["loaded"] == 1
    assert [r[0] for r in engine.connect().execute(text("SELECT id FROM people"))] == [3]
    assert "load_quarantine" in rep["artifacts"]
    assert scalar(engine, "SELECT COUNT(*) FROM etl_quarantined_records WHERE stage='load'") == 2
    assert scalar(engine, f"SELECT COUNT(*) FROM etl_batch_log WHERE status='FAILED'") == 1
    assert scalar(engine, "SELECT COUNT(*) FROM etl_schema_metadata") == 0       # baseline only after a clean load


def test_full_load_failure_marks_run_failed_and_preserves_data(cfg, settings, engine, csv_file):
    run(cfg(), settings, engine)
    with engine.begin() as c:
        c.execute(text("DROP TABLE people"))
        c.execute(text("CREATE TABLE people (id BIGINT PRIMARY KEY, name TEXT, email TEXT CHECK (email <> 'cy@x.com'))"))
        c.execute(text("INSERT INTO people VALUES (99,'keep','keep@x.com')"))
    rep = run(cfg(load={"mode": "full", "batch_size": 2}), settings, engine)
    assert rep["status"] == "FAILED" and "target table unchanged" in rep["error"]   # failed at the atomic swap
    assert [r[0] for r in engine.connect().execute(text("SELECT id FROM people"))] == [99]
    assert scalar(engine, "SELECT COUNT(*) FROM etl_batch_log WHERE status='ROLLED_BACK'") == 2


def test_incremental_load_uses_watermark(make_config, settings, engine, tmp_path):
    p = tmp_path / "o.csv"
    p.write_text("id,day\n1,2024-01-01\n2,2024-01-02\n3,2024-01-03\n")

    def cfg():
        return make_config(source={"type": "csv", "path": str(p)},
                           transformations={"type_conversions": {"id": "integer"}, "date_columns": ["day"]},
                           quality_checks={"primary_key": "id"},
                           load={"mode": "upsert", "batch_size": 10, "watermark_column": "day"})
    first = run(cfg(), settings, engine)
    assert first["counts"]["loaded"] == 3 and first["incremental"]["new_watermark"].startswith("2024-01-03")
    p.write_text(p.read_text() + "4,2024-01-04\n5,2024-01-05\n")
    second = run(cfg(), settings, engine)
    assert second["incremental"]["rows_skipped_by_watermark"] == 2
    assert second["counts"]["transformed"] == 3 and scalar(engine, "SELECT COUNT(*) FROM people") == 5
    third = run(cfg(), settings, engine)
    assert scalar(engine, "SELECT COUNT(*) FROM people") == 5 and third["status"] == "SUCCESS"


def test_watermark_not_advanced_when_a_batch_fails(make_config, settings, engine, tmp_path):
    p = tmp_path / "o.csv"
    p.write_text("id,day\n1,2024-01-01\n2,2024-01-02\n")
    with engine.begin() as c:
        c.execute(text("CREATE TABLE people (id BIGINT PRIMARY KEY, day DATETIME CHECK (id <> 2))"))
    config = make_config(source={"type": "csv", "path": str(p)},
                         transformations={"type_conversions": {"id": "integer"}, "date_columns": ["day"]},
                         quality_checks={"primary_key": "id"}, load={"mode": "upsert", "batch_size": 1,
                                                                     "watermark_column": "day"})
    rep = run(config, settings, engine)
    assert rep["status"] == "PARTIAL_SUCCESS"
    assert scalar(engine, "SELECT COUNT(*) FROM etl_watermarks") == 0


def test_referential_check_runs_against_loaded_table(make_config, settings, engine, tmp_path):
    with engine.begin() as c:
        c.execute(text("CREATE TABLE customers (customer_id BIGINT PRIMARY KEY)"))
        c.execute(text("INSERT INTO customers VALUES (1)"))
    p = tmp_path / "ord.csv"
    p.write_text("id,customer_id\n1,1\n2,99\n")
    config = make_config(source={"type": "csv", "path": str(p)},
                         transformations={"type_conversions": {"id": "integer", "customer_id": "integer"}},
                         quality_checks={"primary_key": "id", "referential_checks": [
                             {"column": "customer_id", "reference_table": "customers", "reference_column": "customer_id"}]})
    rep = run(config, settings, engine)
    assert rep["counts"]["loaded"] == 1 and rep["counts"]["invalid"] == 1
    # and a missing reference table fails the run with a clear message instead of loading unchecked data
    with engine.begin() as c:
        c.execute(text("DROP TABLE customers"))
    rep2 = run(config, settings, engine)
    assert rep2["status"] == "FAILED" and "reference table" in rep2["error"]
