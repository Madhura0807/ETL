"""Runs one pipeline end to end:

extract -> infer schema -> transform -> validate -> detect drift -> load -> record/report

Statuses
--------
SUCCESS          everything loaded (or nothing to load)
PARTIAL_SUCCESS  some append/upsert batches failed; successful batches are committed
BLOCKED          deliberately stopped before loading (quality gate or critical schema drift)
FAILED           unexpected error, unreachable database, or an aborted full load
"""
from __future__ import annotations

import logging
import uuid

import pandas as pd
from pandas.api import types as pdt

from src.config_loader import settings_paths
from src.db.connection import build_engine, check_connection
from src.db.repository import MetadataRepository
from src.exceptions import ETLError
from src.extractors import build_extractor
from src.loaders.mysql_loader import MySQLLoader
from src.monitoring.logger import setup_logging
from src.monitoring.report_generator import build_report, write_report
from src.pipeline.quarantine import frame_to_rows, rejected_to_frame, write_quarantine_file
from src.quality.validator import DataQualityValidator
from src.schema.drift_detector import detect_drift
from src.schema.schema_inference import infer_schema
from src.schema.schema_store import SchemaStore
from src.transformations.transformer import Transformer
from src.utils.time import utcnow


def _cast_watermark(series: pd.Series, raw: str):
    if pdt.is_datetime64_any_dtype(series):
        return pd.Timestamp(raw)
    if pdt.is_numeric_dtype(series):
        return float(raw)
    return raw


class PipelineOrchestrator:
    def __init__(self, config: dict, settings: dict, engine=None, accept_drift: bool = False, sleep=None):
        self.cfg, self.settings = config, settings
        self.engine = engine
        self.accept_drift = accept_drift
        self.sleep = sleep
        self.paths = settings_paths(settings)

    # ------------------------------------------------------------------ run
    def run(self) -> dict:
        cfg = self.cfg
        name = cfg["name"]
        execution_id = str(uuid.uuid4())
        self.log, log_path = setup_logging(self.settings, self.paths["logs"], name, execution_id)
        ctx = {
            "pipeline_name": name, "execution_id": execution_id, "start_time": utcnow(),
            "source_type": cfg["source"]["type"], "status": "RUNNING", "extracted": 0, "transformed": 0,
            "valid": 0, "invalid": 0, "loaded": 0, "failed_batches": 0, "schema_drift_status": "NOT_EVALUATED",
            "warnings": [], "artifacts": {"log_file": str(log_path)}, "error": None,
        }
        repo = None
        run_registered = False
        self.log.info("=== Pipeline '%s' started (execution %s) ===", name, execution_id)
        try:
            if self.engine is None:
                self.engine = build_engine(self.settings)
            check_connection(self.engine, cfg["retry"], self.log, self.sleep)
            repo = MetadataRepository(self.engine)
            repo.create_tables()
            repo.start_run(execution_id, name, ctx["source_type"], ctx["start_time"])
            run_registered = True
            self._execute(ctx, repo)
        except Exception as exc:  # noqa: BLE001 - last line of defence; run must always report
            ctx["status"] = "FAILED"
            ctx["error"] = f"{exc.__class__.__name__}: {exc}"
            self.log.exception("Pipeline failed: %s", exc)
        finally:
            ctx["end_time"] = utcnow()
            report = build_report(ctx)
            report_path = write_report(report, self.paths["reports"])
            report["artifacts"]["report_file"] = str(report_path)
            write_report(report, self.paths["reports"])
            if run_registered and repo is not None:
                try:
                    repo.finish_run(execution_id, report)
                except Exception:  # noqa: BLE001
                    self.log.exception("Could not write final run record to etl_pipeline_runs")
            self.log.info("=== Pipeline '%s' finished: %s in %.2fs (extracted=%d valid=%d invalid=%d loaded=%d "
                          "failed_batches=%d) ===", name, report["status"], report["duration_seconds"],
                          ctx["extracted"], ctx["valid"], ctx["invalid"], ctx["loaded"], ctx["failed_batches"])
        return report

    # -------------------------------------------------------------- stages
    def _execute(self, ctx: dict, repo: MetadataRepository) -> None:
        cfg, log, name, eid = self.cfg, self.log, self.cfg["name"], ctx["execution_id"]

        # Stage 1: extraction
        extraction = build_extractor(cfg["source"], cfg["retry"], log, self.sleep).extract()
        raw = extraction.data
        ctx["extracted"] = len(raw)
        ctx["extraction"] = extraction.metadata
        extraction_rejected = len(extraction.rejected)
        if extraction.rejected:
            frame = rejected_to_frame(extraction.rejected)
            ctx["artifacts"]["extraction_quarantine"] = self._quarantine(frame, "_reason", "extraction", ctx, repo)
        if raw.empty:
            ctx["status"] = "SUCCESS"
            ctx["invalid"] = extraction_rejected
            ctx["warnings"].append("source returned no usable rows; nothing loaded and target left untouched")
            log.warning(ctx["warnings"][-1])
            return

        # Stage 2: schema inference (on the data exactly as extracted)
        schema = infer_schema(raw)
        log.info("Inferred schema: %d columns", schema["column_count"])

        # Stage 3: transformation (+ incremental watermark filter)
        tres = Transformer(cfg["transformations"], log).transform(raw)
        df = tres.data
        ctx["transform_stats"] = tres.stats
        wm_col = cfg["load"].get("watermark_column")
        new_watermark = None
        if wm_col:
            if wm_col not in df.columns:
                raise ETLError(f"watermark column '{wm_col}' not found after transformation")
            last = repo.get_watermark(name)
            before = len(df)
            if last is not None:
                bound = _cast_watermark(df[wm_col], last)
                df = df[df[wm_col].isna() | (df[wm_col] >= bound)]  # '>=' + idempotent writes = safe boundary
            ctx["incremental"] = {"watermark_column": wm_col, "previous_watermark": last,
                                  "rows_skipped_by_watermark": before - len(df)}
            log.info("Incremental load: watermark=%s, skipped %d already-seen rows", last, before - len(df))
        ctx["transformed"] = len(df)

        # Stage 4: data-quality validation
        vres = DataQualityValidator(cfg["quality_checks"], self.engine, log).validate(df, tres.conversion_failures)
        ctx["valid"], ctx["invalid"] = len(vres.valid), len(vres.invalid) + extraction_rejected
        ctx["quality_results"] = [r.to_dict() for r in vres.results]
        repo.save_quality_results(eid, ctx["quality_results"])
        if not vres.invalid.empty:
            ctx["artifacts"]["validation_quarantine"] = self._quarantine(
                vres.invalid, "_dq_reasons", "validation", ctx, repo)
        ctx["warnings"] += [f"quality check {r.check_name} {r.status}: {r.failed} failed"
                            for r in vres.results if r.status == "WARNING"]

        # Stage 5: schema drift detection
        store = SchemaStore(self.paths["schemas"], name)
        prev = store.latest() or repo.latest_schema(name)
        prev_version, prev_schema = prev if prev else (None, None)
        drift = detect_drift(prev_schema, schema, cfg["schema_drift"]["policies"], self.accept_drift, prev_version)
        drift_dict = drift.to_dict()
        ctx["schema_drift"] = drift_dict
        ctx["schema_drift_status"] = drift.status
        if drift.has_changes:
            ctx["artifacts"]["drift_report"] = str(store.save_drift_report(drift_dict, eid))
            repo.save_drift(name, eid, drift_dict)
            for ch in drift.changes:
                level = logging.ERROR if ch.severity == "CRITICAL" else logging.WARNING if ch.severity == "WARNING" else logging.INFO
                log.log(level, "Schema drift [%s] %s -> action=%s", ch.severity, ch.message, ch.action)

        blockers = []
        if not vres.gate_passed:
            blockers += [f"quality gate: {m}" for m in vres.gate_failures]
        if drift.blocked:
            blockers += [f"schema drift: {m}" for m in drift.blocked_reasons]
        if blockers:
            ctx["status"] = "BLOCKED"
            ctx["error"] = " || ".join(blockers)
            if drift.blocked:
                ctx["warnings"].append("load skipped; baseline schema NOT updated. Use --accept-drift to accept "
                                       "a reviewed schema change.")
            log.error("Load BLOCKED: %s", ctx["error"])
            return

        # Stage 6: loading
        reverse = {raw_name: std for raw_name, std in tres.column_map.items()}
        exclude = {reverse.get(c, c) for c in drift.exclude_columns}
        add = {reverse.get(c, c) for c in drift.add_columns}
        to_load = vres.valid.drop(columns=[c for c in exclude if c in vres.valid.columns])
        if exclude:
            ctx["warnings"].append(f"new source column(s) excluded from load by drift policy: {sorted(exclude)}")
        if wm_col and wm_col in to_load.columns and to_load[wm_col].notna().any():
            new_watermark = to_load[wm_col].max()
        if cfg["output"]["save_processed"] and not to_load.empty:
            self.paths["processed"].mkdir(parents=True, exist_ok=True)
            p = self.paths["processed"] / f"{name}_{eid[:8]}.csv"
            to_load.to_csv(p, index=False)
            ctx["artifacts"]["processed_file"] = str(p)

        date_cols = set(cfg["transformations"].get("date_columns") or []) | {
            c for c, t in (cfg["transformations"].get("type_conversions") or {}).items() if t == "date"}
        loader = MySQLLoader(
            self.engine, cfg["target"]["table"], cfg["load"]["mode"], cfg["load"]["batch_size"],
            cfg["quality_checks"]["primary_key"], cfg["load"]["on_duplicate"], cfg["retry"], repo, name,
            date_columns=date_cols, logger=log, **({"sleep": self.sleep} if self.sleep else {}))
        lres = loader.load(to_load, eid, add_columns=add)
        ctx["loaded"] = lres.loaded_count
        ctx["failed_batches"] = lres.failed_batch_count
        ctx["load"] = {"mode": lres.mode, "table": lres.table, "rows_attempted": lres.rows_attempted,
                       "aborted": lres.aborted, "batches": [b.to_dict() for b in lres.batches]}
        if not lres.failed_rows.empty:
            ctx["artifacts"]["load_quarantine"] = self._quarantine(lres.failed_rows, "_load_error", "load", ctx, repo)
            ctx["invalid"] += len(lres.failed_rows)

        # Stage 7: outcome, baseline + watermark bookkeeping
        if lres.aborted:
            ctx["status"] = "FAILED"
            ctx["error"] = "full load aborted: a batch failed while staging; existing target data left untouched"
        elif lres.failed_batch_count:
            ctx["status"] = "PARTIAL_SUCCESS"
            ctx["error"] = f"{lres.failed_batch_count} batch(es) failed; failed rows quarantined (stage=load)"
        else:
            ctx["status"] = "SUCCESS"
            # Columns excluded by policy are NOT part of the accepted baseline, so they keep being
            # reported as drift on every run until someone chooses 'add' (or removes them upstream).
            accepted = [c for c in drift.changes if not (c.change_type == "column_added" and c.action == "exclude")]
            if prev_schema is None or accepted:
                baseline = {**schema, "columns": {k: v for k, v in schema["columns"].items()
                                                  if k not in drift.exclude_columns},
                            "excluded_columns": drift.exclude_columns}
                baseline["column_count"] = len(baseline["columns"])
                version = store.save(baseline, eid)
                repo.save_schema_version(name, version, eid, {**baseline, "schema_version": version})
                ctx["schema_version"] = version
                log.info("Saved schema version %d", version)
            else:
                ctx["schema_version"] = prev_version
            if wm_col and new_watermark is not None:
                repo.set_watermark(name, wm_col, new_watermark, eid)
                ctx["incremental"]["new_watermark"] = str(new_watermark)

    # ------------------------------------------------------------ helpers
    def _quarantine(self, frame: pd.DataFrame, reason_col: str, stage: str, ctx: dict, repo) -> str | None:
        path = write_quarantine_file(frame, self.paths["quarantine"], ctx["pipeline_name"], ctx["execution_id"], stage)
        cap = self.settings["quarantine"]["max_db_rows"]
        repo.save_quarantine(ctx["execution_id"], ctx["pipeline_name"], stage, frame_to_rows(frame.head(cap), reason_col))
        if len(frame) > cap:
            ctx["warnings"].append(f"{stage} quarantine: only first {cap} of {len(frame)} rows stored in DB "
                                   "(full set in CSV file)")
        self.log.warning("Quarantined %d record(s) at stage '%s' -> %s", len(frame), stage, path)
        return str(path) if path else None
