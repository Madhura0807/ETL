from __future__ import annotations

import json

import pandas as pd

from src.exceptions import ExtractionError
from src.extractors.base import BaseExtractor
from src.utils.paths import resolve_path


def navigate(obj, dotted: str | None, what: str):
    """Follow a dotted key path (``a.b.c``) into nested dicts."""
    if not dotted:
        return obj
    for key in dotted.split("."):
        if isinstance(obj, dict) and key in obj:
            obj = obj[key]
        else:
            raise ExtractionError(f"{what}: key '{key}' (path '{dotted}') not found")
    return obj


def records_to_frame(records: list, flatten: bool, rejected: list[dict]) -> pd.DataFrame:
    """Keep dict records, reject the rest, flatten nested dicts (a.b -> a_b)."""
    good = []
    for i, rec in enumerate(records):
        if isinstance(rec, dict):
            good.append(rec)
        else:
            rejected.append({"reason": f"record {i} is not a JSON object", "record": json.dumps(rec, default=str)[:500]})
    if not good:
        return pd.DataFrame()
    df = pd.json_normalize(good, sep="_") if flatten else pd.DataFrame(good)
    # remaining lists/dicts become JSON text so they are storable and comparable
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].map(lambda v: json.dumps(v, sort_keys=True) if isinstance(v, (list, dict)) else v)
    return df


class JSONExtractor(BaseExtractor):
    source_type = "json"

    def _extract(self):
        path = resolve_path(self.config["path"])
        if not path.exists():
            raise ExtractionError(f"JSON file not found: {path}")
        try:
            text = path.read_text(encoding=self.config.get("encoding", "utf-8"))
        except UnicodeDecodeError as exc:
            raise ExtractionError(f"JSON file is not valid text: {path}") from exc
        rejected: list[dict] = []
        if self.config.get("lines"):
            records = []
            for n, line in enumerate(text.splitlines(), 1):
                if not line.strip():
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError as exc:
                    rejected.append({"reason": f"malformed JSON on line {n}: {exc.msg}", "record": line[:500]})
        else:
            try:
                payload = json.loads(text)
            except json.JSONDecodeError as exc:
                raise ExtractionError(f"malformed JSON in {path}: {exc}") from exc
            records = navigate(payload, self.config.get("record_path"), "JSON record_path")
            if isinstance(records, dict):
                records = [records]
            if not isinstance(records, list):
                raise ExtractionError("JSON records must be a list (or an object) at the configured record_path")
        df = records_to_frame(records, self.config.get("flatten", True), rejected)
        return df, rejected, {"path": str(path), "file_size_bytes": path.stat().st_size}
