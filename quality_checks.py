"""Individual data-quality checks. Each returns (CheckResult, {row_index: reason}).

Row-level checks return the failing rows (they get quarantined). Dataset-level checks
(``level == 'dataset'``) judge the data as a whole and can stop the pipeline (quality gate).
"""
from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field

import pandas as pd
from pandas.api import types as pdt

EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9\-]+(\.[A-Za-z0-9\-]+)*\.[A-Za-z]{2,}$")


@dataclass
class CheckResult:
    check_name: str
    level: str                 # row | dataset
    total_checked: int
    passed: int
    failed: int
    failure_pct: float
    error_details: list = field(default_factory=list)
    unit: str = "records"      # 'records' for row checks; 'columns' etc. for dataset checks
    status: str = "PASSED"     # PASSED | FAILED | WARNING

    def to_dict(self) -> dict:
        return asdict(self)


def _row_result(name: str, total: int, reasons: dict, max_details: int) -> tuple[CheckResult, dict]:
    failed = len(reasons)
    details = [f"row {idx}: {msg}" for idx, msg in list(reasons.items())[:max_details]]
    if failed > max_details:
        details.append(f"... and {failed - max_details} more")
    return CheckResult(name, "row", total, total - failed, failed,
                       round(100 * failed / total, 2) if total else 0.0, details,
                       status="FAILED" if failed else "PASSED"), reasons


def _dataset_result(name: str, total: int, failed: int, details: list, unit: str, warn_only=False) -> CheckResult:
    status = "PASSED" if not failed else ("WARNING" if warn_only else "FAILED")
    return CheckResult(name, "dataset", total, total - failed, failed,
                       round(100 * failed / total, 2) if total else 0.0, details, unit, status)


def missing_column(name: str, column: str) -> tuple[CheckResult, dict]:
    return _dataset_result(name, 1, 1, [f"column '{column}' not found in data"], "columns"), {}


# ---------------------------------------------------------------- dataset level
def check_required_columns(df, required: list[str]):
    missing = [c for c in required if c not in df.columns]
    return _dataset_result("required_columns_present", len(required), len(missing),
                           [f"missing required column '{c}'" for c in missing], "columns"), {}


def check_null_threshold(df, columns: list[str], threshold: float, action: str):
    cols = [c for c in columns if c in df.columns]
    breaches = []
    for c in cols:
        ratio = float(df[c].isna().mean()) if len(df) else 0.0
        if ratio > threshold:
            breaches.append(f"column '{c}': {ratio:.1%} nulls exceeds threshold {threshold:.1%}")
    return _dataset_result("null_threshold", len(cols), len(breaches), breaches, "columns",
                           warn_only=(action == "warn")), {}


# ------------------------------------------------------------------- row level
def check_required_nulls(df, required: list[str], max_details: int):
    present = [c for c in required if c in df.columns]
    reasons = {}
    if present:
        null_mask = df[present].isna()
        for idx in df.index[null_mask.any(axis=1)]:
            cols = [c for c in present if null_mask.at[idx, c]]
            reasons[idx] = f"null in required column(s): {', '.join(cols)}"
    return _row_result("null_check_required", len(df), reasons, max_details)


def check_duplicate_rows(df, max_details: int):
    dup = df.duplicated(keep="first")
    return _row_result("duplicate_rows", len(df), {i: "exact duplicate of an earlier row" for i in df.index[dup]},
                       max_details)


def check_primary_key_unique(df, pk: list[str], policy: str, max_details: int):
    reasons = {}
    if all(c in df.columns for c in pk):
        keyed = df[df[pk].notna().all(axis=1)]  # null keys are the required-null check's job
        keep = False if policy == "quarantine_all" else ("last" if policy == "keep_last" else "first")
        dup = keyed.duplicated(subset=pk, keep=keep)
        for idx in keyed.index[dup]:
            reasons[idx] = f"duplicate primary key {tuple(keyed.at[idx, c] for c in pk)}"
    return _row_result("primary_key_unique", len(df), reasons, max_details)


def _valid_type_mask(s: pd.Series, target: str) -> pd.Series:
    """True where a non-null value is acceptable for ``target`` (nulls -> True)."""
    ok = pd.Series(True, index=s.index)
    nn = s.notna()
    if target == "integer":
        num = pd.to_numeric(s, errors="coerce")
        ok[nn] = num[nn].notna() & (num[nn] % 1 == 0)
    elif target == "float":
        ok[nn] = pd.to_numeric(s, errors="coerce")[nn].notna()
    elif target == "boolean":
        ok[nn] = s[nn].map(lambda v: isinstance(v, bool) or str(v).strip().lower() in {"true", "false"})
    elif target in {"date", "datetime"}:
        if not pdt.is_datetime64_any_dtype(s):
            ok[nn] = pd.to_datetime(s[nn], errors="coerce", format="mixed", utc=True).notna()
    elif target == "string":
        ok[nn] = s[nn].map(lambda v: isinstance(v, str))
    return ok


def check_data_types(df, column_types: dict, conversion_failures: dict, max_details: int):
    reasons: dict = {}
    for idx, msgs in conversion_failures.items():
        if idx in df.index:
            reasons[idx] = "; ".join(msgs)
    for col, target in column_types.items():
        if col not in df.columns:
            continue
        ok = _valid_type_mask(df[col], target)
        for idx in df.index[~ok]:
            msg = f"column '{col}': value {df.at[idx, col]!r} is not a valid {target}"
            reasons[idx] = f"{reasons[idx]}; {msg}" if idx in reasons else msg
    return _row_result("data_type_validation", len(df), reasons, max_details)


def check_range(df, col: str, rule: dict, max_details: int):
    reasons = {}
    num = pd.to_numeric(df[col], errors="coerce")
    for idx in df.index[df[col].notna() & num.isna()]:
        reasons[idx] = f"{col}={df.at[idx, col]!r} is not numeric"
    lo, hi = rule.get("min"), rule.get("max")
    if lo is not None:
        for idx in df.index[num < lo]:
            reasons[idx] = f"{col}={num[idx]} below minimum {lo}"
    if hi is not None:
        for idx in df.index[num > hi]:
            reasons[idx] = f"{col}={num[idx]} above maximum {hi}"
    return _row_result(f"range_check:{col}", len(df), reasons, max_details)


def check_email(df, col: str, max_details: int):
    reasons = {}
    for idx, v in df[col].dropna().items():
        if not (isinstance(v, str) and EMAIL_RE.match(v.strip())):
            reasons[idx] = f"invalid email format {v!r}"
    return _row_result(f"email_format:{col}", len(df), reasons, max_details)


def _bound(value):
    if isinstance(value, str) and value.strip().lower() == "today":
        return pd.Timestamp.now().normalize()
    return pd.Timestamp(value)


def check_date(df, col: str, rule: dict, max_details: int):
    reasons = {}
    s = df[col]
    parsed = s if pdt.is_datetime64_any_dtype(s) else pd.to_datetime(s, errors="coerce", format="mixed", utc=True).dt.tz_localize(None)
    for idx in df.index[s.notna() & parsed.isna()]:
        reasons[idx] = f"{col}={s[idx]!r} is not a valid date"
    if "min" in rule:
        lo = _bound(rule["min"])
        for idx in df.index[parsed < lo]:
            reasons[idx] = f"{col}={parsed[idx].date()} before minimum {lo.date()}"
    if "max" in rule:
        hi = _bound(rule["max"])
        for idx in df.index[parsed > hi]:
            reasons[idx] = f"{col}={parsed[idx].date()} after maximum {hi.date()}"
    return _row_result(f"date_check:{col}", len(df), reasons, max_details)


def check_referential(df, col: str, ref_table: str, ref_col: str, ref_values: set, max_details: int):
    reasons = {}
    for idx, v in df[col].dropna().items():
        key = v.item() if hasattr(v, "item") else v
        if key not in ref_values:
            reasons[idx] = f"{col}={key!r} not found in {ref_table}.{ref_col}"
    return _row_result(f"referential_integrity:{col}->{ref_table}.{ref_col}", len(df), reasons, max_details)


def check_custom_rule(df, name: str, expression: str, max_details: int):
    """``expression`` is a pandas ``df.eval`` boolean expression that must hold for valid rows.

    Expressions come from trusted, version-controlled YAML (see docs: limitations).
    """
    try:
        result = df.eval(expression, engine="python")
        if not pdt.is_bool_dtype(result):
            raise ValueError("expression did not produce a boolean result per row")
        ok = result.fillna(False).astype(bool)
    except Exception as exc:  # noqa: BLE001 - report any evaluation problem as a gate failure
        return _dataset_result(f"custom_rule:{name}", 1, 1, [f"rule could not be evaluated: {exc}"], "rules"), {}
    reasons = {idx: f"custom rule '{name}' failed ({expression})" for idx in df.index[~ok]}
    return _row_result(f"custom_rule:{name}", len(df), reasons, max_details)
