"""Infer logical column types, nullability, uniqueness and basic statistics from a dataframe."""
from __future__ import annotations

import datetime as dt
import decimal
import re

import pandas as pd
from pandas.api import types as pdt

from src.utils.time import utcnow

INT_RE = re.compile(r"^[+-]?(0|[1-9]\d*)$")
FLOAT_RE = re.compile(r"^[+-]?(\d+\.\d*|\.\d+)([eE][+-]?\d+)?$|^[+-]?\d+[eE][+-]?\d+$")
DATE_RES = [
    re.compile(r"^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:?\d{2})?)?$"),
    re.compile(r"^\d{1,2}/\d{1,2}/\d{4}$"),
    re.compile(r"^\d{1,2}-\d{1,2}-\d{4}$"),
]
BOOL_WORDS = {"true", "false"}


def _infer_object_type(values: pd.Series) -> str:
    vals = [v.strip() if isinstance(v, str) else v for v in values]
    if all(isinstance(v, bool) for v in vals):
        return "boolean"
    if all(isinstance(v, int) and not isinstance(v, bool) for v in vals):
        return "integer"
    if all(isinstance(v, (int, float, decimal.Decimal)) and not isinstance(v, bool) for v in vals):
        return "float"
    if all(isinstance(v, (dt.date, dt.datetime, pd.Timestamp)) for v in vals):
        return "datetime"
    if not all(isinstance(v, str) for v in vals):
        return "string"
    if all(INT_RE.match(v) for v in vals):
        return "integer"
    if all(INT_RE.match(v) or FLOAT_RE.match(v) for v in vals):
        return "float"
    if all(v.lower() in BOOL_WORDS for v in vals):
        return "boolean"
    if all(any(r.match(v) for r in DATE_RES) for v in vals):
        parsed = pd.to_datetime(pd.Series(vals), errors="coerce", format="mixed", utc=True)
        if parsed.notna().all():
            return "datetime"
    return "string"


def infer_logical_type(non_null: pd.Series) -> str:
    """integer | float | string | boolean | datetime | unknown (all values null)."""
    if non_null.empty:
        return "unknown"
    if pdt.is_bool_dtype(non_null):
        return "boolean"
    if pdt.is_integer_dtype(non_null):
        return "integer"
    if pdt.is_float_dtype(non_null):
        return "float"
    if pdt.is_datetime64_any_dtype(non_null):
        return "datetime"
    return _infer_object_type(non_null)


def _stats(non_null: pd.Series, ltype: str) -> dict:
    if non_null.empty:
        return {}
    if ltype in {"integer", "float"}:
        num = pd.to_numeric(non_null, errors="coerce").dropna()
        if num.empty:
            return {}
        conv = int if ltype == "integer" else float
        return {"min": conv(num.min()), "max": conv(num.max()), "mean": round(float(num.mean()), 4)}
    if ltype == "datetime":
        parsed = pd.to_datetime(non_null, errors="coerce", format="mixed", utc=True).dropna()
        if parsed.empty:
            return {}
        return {"min": parsed.min().tz_localize(None).isoformat(), "max": parsed.max().tz_localize(None).isoformat()}
    if ltype == "boolean":
        return {"true_count": int(non_null.map(lambda v: str(v).strip().lower() == "true").sum())}
    lengths = non_null.map(lambda v: len(str(v)))
    return {"min_length": int(lengths.min()), "max_length": int(lengths.max())}


def infer_schema(df: pd.DataFrame) -> dict:
    """Return a JSON-serialisable schema document for ``df``."""
    n = len(df)
    columns: dict[str, dict] = {}
    for pos, col in enumerate(df.columns):
        s = df[col]
        non_null = s.dropna()
        ltype = infer_logical_type(non_null)
        nulls = int(s.isna().sum())
        columns[str(col)] = {
            "position": pos,
            "type": ltype,
            "nullable": nulls > 0,
            "null_count": nulls,
            "null_pct": round(nulls / n, 4) if n else 0.0,
            "unique": bool(len(non_null) > 0 and non_null.is_unique),
            "distinct_count": int(non_null.astype(str).nunique()),
            "stats": _stats(non_null, ltype),
        }
    return {
        "inferred_at": utcnow().isoformat(),
        "row_count": int(n),
        "column_count": int(len(df.columns)),
        "columns": columns,
    }
