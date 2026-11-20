"""Config-driven, reusable transformations. Contains NO validation or loading logic.

Column names in the config refer to names AFTER standardisation (when enabled).
Values that cannot be converted become null AND are recorded in ``conversion_failures`` so
the validator can quarantine those rows instead of silently losing the information.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

import numpy as np
import pandas as pd
from pandas.api import types as pdt

TRUE_WORDS = {"true", "t", "yes", "y", "1"}
FALSE_WORDS = {"false", "f", "no", "n", "0"}


def standardize_column_name(name) -> str:
    """'Customer ID' / 'customerId' / ' E-mail ' -> 'customer_id' / 'customer_id' / 'e_mail'."""
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", str(name).strip())
    s = re.sub(r"[^0-9a-zA-Z]+", "_", s.lower()).strip("_")
    if not s:
        s = "column"
    if s[0].isdigit():
        s = f"col_{s}"
    return s


def column_name_map(columns) -> dict[str, str]:
    """Raw -> standardised names; collisions get _2, _3 ... so no column is overwritten."""
    seen: dict[str, int] = {}
    out: dict[str, str] = {}
    for raw in columns:
        base = standardize_column_name(raw)
        seen[base] = seen.get(base, 0) + 1
        out[str(raw)] = base if seen[base] == 1 else f"{base}_{seen[base]}"
    return out


@dataclass
class TransformResult:
    data: pd.DataFrame
    conversion_failures: dict = field(default_factory=dict)  # row index -> [messages]
    column_map: dict = field(default_factory=dict)
    stats: dict = field(default_factory=dict)


def _is_text(s: pd.Series) -> bool:
    return pdt.is_object_dtype(s) or pdt.is_string_dtype(s)


def convert_series(series: pd.Series, target: str, dayfirst: bool = False):
    """Convert to ``target`` type. Returns (converted, bad_mask)."""
    if target == "integer":
        num = pd.to_numeric(series, errors="coerce")
        bad = (series.notna() & num.isna()) | (num.notna() & (num % 1 != 0))
        return num.mask(bad).astype("Int64"), bad
    if target == "float":
        num = pd.to_numeric(series, errors="coerce")
        return num.astype("float64"), series.notna() & num.isna()
    if target == "boolean":
        def to_bool(v):
            if pd.isna(v):
                return pd.NA
            if isinstance(v, (bool, np.bool_)):
                return bool(v)
            t = str(v).strip().lower()
            return True if t in TRUE_WORDS else False if t in FALSE_WORDS else pd.NA
        out = series.map(to_bool).astype("boolean")
        return out, series.notna() & out.isna()
    if target in {"date", "datetime"}:
        out = pd.to_datetime(series, errors="coerce", dayfirst=dayfirst, utc=True, format="mixed")
        out = out.dt.tz_localize(None)
        if target == "date":
            out = out.dt.normalize()
        return out, series.notna() & out.isna()
    if target == "string":
        return series.map(lambda v: v if pd.isna(v) else str(v)).astype(object), pd.Series(False, index=series.index)
    raise ValueError(f"unsupported target type {target}")


class Transformer:
    def __init__(self, config: dict, logger: logging.Logger | None = None):
        self.cfg = config
        self.log = logger or logging.getLogger("etl.transform")

    def _col(self, df, col, step) -> bool:
        if col in df.columns:
            return True
        self.log.warning("Transformation '%s' skipped for missing column '%s'", step, col)
        return False

    def transform(self, df: pd.DataFrame) -> TransformResult:
        cfg = self.cfg
        df = df.copy()
        stats = {"input_rows": len(df), "steps": []}
        failures: dict = {}
        col_map = {str(c): str(c) for c in df.columns}

        if cfg.get("standardize_columns"):
            col_map = column_name_map(df.columns)
            df.columns = [col_map[str(c)] for c in df.columns]
            stats["steps"].append("standardize_columns")

        if cfg.get("trim_strings"):
            for col in df.columns:
                if _is_text(df[col]):
                    df[col] = df[col].map(
                        lambda v: (np.nan if v.strip() == "" else v.strip()) if isinstance(v, str) else v)
            stats["steps"].append("trim_strings")

        conversions = dict(cfg.get("type_conversions") or {})
        for col in cfg.get("date_columns") or []:
            conversions.setdefault(col, "date")
        dayfirst = bool(cfg.get("date_dayfirst", False))
        for col, target in conversions.items():
            if not self._col(df, col, f"convert->{target}"):
                continue
            original = df[col]
            df[col], bad = convert_series(original, target, dayfirst)
            for idx in df.index[bad.fillna(False).astype(bool)]:
                failures.setdefault(idx, []).append(f"column '{col}': cannot convert {original[idx]!r} to {target}")
        if conversions:
            stats["steps"].append("type_conversions")
        stats["conversion_failure_rows"] = len(failures)

        mv = cfg.get("missing_values") or {}
        for col, value in (mv.get("fill") or {}).items():
            if self._col(df, col, "fill"):
                df[col] = df[col].fillna(value)
        for col, strat in (mv.get("strategies") or {}).items():
            if not self._col(df, col, f"fill_{strat}"):
                continue
            s = df[col]
            if strat == "mode":
                modes = s.dropna().mode()
                fill = modes.iloc[0] if len(modes) else None
            else:
                num = pd.to_numeric(s, errors="coerce")
                fill = num.mean() if strat == "mean" else num.median()
                if pdt.is_integer_dtype(s) and pd.notna(fill):
                    fill = int(round(fill))
            if fill is not None and not pd.isna(fill):
                df[col] = s.fillna(fill)
        if mv:
            stats["steps"].append("missing_values")

        norm = cfg.get("normalize") or {}
        for key, fn in (("lowercase", str.lower), ("uppercase", str.upper), ("titlecase", str.title)):
            for col in norm.get(key, []):
                if self._col(df, col, key):
                    df[col] = df[col].map(lambda v, f=fn: f(v) if isinstance(v, str) else v)
        if norm:
            stats["steps"].append("normalize")

        rd = cfg.get("remove_duplicates")
        stats["duplicates_removed"] = 0
        if rd:
            subset = rd.get("subset") if isinstance(rd, dict) else None
            keep = rd.get("keep", "first") if isinstance(rd, dict) else "first"
            if subset:
                subset = [c for c in subset if self._col(df, c, "remove_duplicates")] or None
            dup = df.duplicated(subset=subset, keep=keep)
            stats["duplicates_removed"] = int(dup.sum())
            df = df[~dup]
            stats["steps"].append("remove_duplicates")
            if stats["duplicates_removed"]:
                self.log.info("Removed %d duplicate row(s)", stats["duplicates_removed"])

        failures = {i: m for i, m in failures.items() if i in df.index}
        stats["output_rows"] = len(df)
        return TransformResult(df, failures, col_map, stats)
