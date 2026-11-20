"""Runs the configured checks, separates valid from invalid rows, and applies the quality gate."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

import pandas as pd
from sqlalchemy import MetaData, Table, inspect, select

from src.exceptions import ReferenceDataError
from src.quality import quality_checks as qc


@dataclass
class ValidationResult:
    valid: pd.DataFrame
    invalid: pd.DataFrame                       # original columns + '_dq_reasons'
    results: list[qc.CheckResult] = field(default_factory=list)
    gate_failures: list[str] = field(default_factory=list)  # dataset-level failures => do not load

    @property
    def gate_passed(self) -> bool:
        return not self.gate_failures


class DataQualityValidator:
    def __init__(self, config: dict, engine=None, logger: logging.Logger | None = None):
        self.cfg = config
        self.engine = engine
        self.log = logger or logging.getLogger("etl.quality")

    def _reference_values(self, table: str, column: str) -> set:
        if self.engine is None or not inspect(self.engine).has_table(table):
            raise ReferenceDataError(
                f"reference table '{table}' does not exist; run the pipeline that loads it first")
        t = Table(table, MetaData(), autoload_with=self.engine)
        if column not in t.c:
            raise ReferenceDataError(f"reference column '{table}.{column}' does not exist")
        with self.engine.connect() as conn:
            return {row[0] for row in conn.execute(select(t.c[column]).distinct()) if row[0] is not None}

    def validate(self, df: pd.DataFrame, conversion_failures: dict | None = None) -> ValidationResult:
        cfg, md = self.cfg, self.cfg["max_error_details"]
        row_reasons: dict = {}
        results: list[qc.CheckResult] = []
        gate: list[str] = []

        def add(pair):
            result, reasons = pair
            results.append(result)
            for idx, msg in reasons.items():
                row_reasons.setdefault(idx, []).append(f"[{result.check_name}] {msg}")
            if result.level == "dataset" and result.status == "FAILED":
                gate.append(f"{result.check_name}: " + "; ".join(result.error_details))

        required = list(dict.fromkeys(cfg["required_columns"] + cfg["primary_key"]))
        if required:
            add(qc.check_required_columns(df, required))
            add(qc.check_required_nulls(df, required, md))
        if cfg["primary_key"]:
            add(qc.check_primary_key_unique(df, cfg["primary_key"], cfg["primary_key_policy"], md))
        if cfg["check_duplicates"]:
            add(qc.check_duplicate_rows(df, md))
        add(qc.check_data_types(df, cfg["column_types"], conversion_failures or {}, md))

        if cfg["null_threshold"] < 1.0:
            cols = cfg["null_threshold_columns"] or list(df.columns)
            add(qc.check_null_threshold(df, cols, cfg["null_threshold"], cfg["null_threshold_action"]))
        for col, rule in cfg["range_checks"].items():
            add(qc.check_range(df, col, rule, md) if col in df.columns else qc.missing_column(f"range_check:{col}", col))
        for col in cfg["email_columns"]:
            add(qc.check_email(df, col, md) if col in df.columns else qc.missing_column(f"email_format:{col}", col))
        for col, rule in cfg["date_checks"].items():
            add(qc.check_date(df, col, rule, md) if col in df.columns else qc.missing_column(f"date_check:{col}", col))
        for ref in cfg["referential_checks"]:
            col = ref["column"]
            if col not in df.columns:
                add(qc.missing_column(f"referential_integrity:{col}", col))
                continue
            values = self._reference_values(ref["reference_table"], ref["reference_column"])
            add(qc.check_referential(df, col, ref["reference_table"], ref["reference_column"], values, md))
        for rule in cfg["custom_rules"]:
            add(qc.check_custom_rule(df, rule["name"], rule["expression"], md))

        bad_idx = [i for i in df.index if i in row_reasons]
        invalid = df.loc[bad_idx].copy()
        invalid["_dq_reasons"] = [" | ".join(row_reasons[i]) for i in bad_idx]
        valid = df.drop(index=bad_idx)
        for r in results:
            if r.status != "PASSED":
                self.log.warning("Quality check %s: %s (%d/%d failed)", r.check_name, r.status, r.failed, r.total_checked)
        self.log.info("Validation: %d valid, %d invalid, gate %s", len(valid), len(invalid),
                      "PASSED" if not gate else "FAILED")
        return ValidationResult(valid, invalid, results, gate)
