"""Versioned schema + drift-report persistence on disk (mirrored to MySQL by the repository)."""
from __future__ import annotations

import json
import re
from pathlib import Path

from src.utils.time import utcnow

_VERSION_RE = re.compile(r"schema_v(\d+)\.json$")


class SchemaStore:
    def __init__(self, schemas_dir: Path, pipeline_name: str):
        self.dir = Path(schemas_dir) / pipeline_name
        self.drift_dir = self.dir / "drift_reports"

    def _versions(self) -> list[tuple[int, Path]]:
        if not self.dir.exists():
            return []
        found = []
        for p in self.dir.glob("schema_v*.json"):
            m = _VERSION_RE.search(p.name)
            if m:
                found.append((int(m.group(1)), p))
        return sorted(found)

    def latest(self) -> tuple[int, dict] | None:
        versions = self._versions()
        if not versions:
            return None
        version, path = versions[-1]
        return version, json.loads(path.read_text(encoding="utf-8"))

    def save(self, schema: dict, execution_id: str) -> int:
        self.dir.mkdir(parents=True, exist_ok=True)
        versions = self._versions()
        version = (versions[-1][0] + 1) if versions else 1
        doc = {**schema, "schema_version": version, "execution_id": execution_id}
        (self.dir / f"schema_v{version:04d}.json").write_text(json.dumps(doc, indent=2), encoding="utf-8")
        return version

    def save_drift_report(self, report: dict, execution_id: str) -> Path:
        self.drift_dir.mkdir(parents=True, exist_ok=True)
        stamp = utcnow().strftime("%Y%m%dT%H%M%S")
        path = self.drift_dir / f"drift_{stamp}_{execution_id[:8]}.json"
        path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        return path
