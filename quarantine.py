"""Quarantine = keep rejected records (file + DB) with the reason, never drop them silently."""
from __future__ import annotations

from pathlib import Path

import pandas as pd


def write_quarantine_file(df: pd.DataFrame, directory: Path, pipeline: str, execution_id: str, stage: str) -> Path | None:
    if df.empty:
        return None
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{pipeline}_{execution_id[:8]}_{stage}.csv"
    df.to_csv(path, index_label="row_index")
    return path


def frame_to_rows(df: pd.DataFrame, reason_col: str) -> list[tuple[str, dict]]:
    """(reason, record) tuples for the DB table; reason column is not repeated in the record."""
    rows = []
    for idx, rec in df.drop(columns=[reason_col]).iterrows():
        record = {"row_index": idx, **{k: (None if pd.isna(v) else v) for k, v in rec.items()}}
        rows.append((str(df.at[idx, reason_col]), record))
    return rows


def rejected_to_frame(rejected: list[dict]) -> pd.DataFrame:
    return pd.DataFrame([{"_reason": r["reason"], "raw_record": r["record"]} for r in rejected])
