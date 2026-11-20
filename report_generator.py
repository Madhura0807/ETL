"""Build and persist the JSON execution report written after every run."""
from __future__ import annotations

import json
from pathlib import Path


def build_report(ctx: dict) -> dict:
    """Assemble the report from the run context collected by the orchestrator."""
    end = ctx["end_time"]
    return {
        "pipeline_name": ctx["pipeline_name"],
        "execution_id": ctx["execution_id"],
        "start_time": ctx["start_time"].isoformat(),
        "end_time": end.isoformat(),
        "end_time_dt": end,
        "duration_seconds": round((end - ctx["start_time"]).total_seconds(), 3),
        "source_type": ctx["source_type"],
        "status": ctx["status"],
        "counts": {
            "extracted": ctx["extracted"], "transformed": ctx["transformed"], "valid": ctx["valid"],
            "invalid": ctx["invalid"], "loaded": ctx["loaded"], "failed_batches": ctx["failed_batches"],
        },
        "schema_drift_status": ctx["schema_drift_status"],
        "schema_version": ctx.get("schema_version"),
        "load": ctx.get("load"),
        "quality_results": ctx.get("quality_results", []),
        "schema_drift": ctx.get("schema_drift"),
        "warnings": ctx.get("warnings", []),
        "extraction": ctx.get("extraction"),
        "transform_stats": ctx.get("transform_stats"),
        "incremental": ctx.get("incremental"),
        "artifacts": ctx.get("artifacts", {}),
        "error": ctx.get("error"),
    }


def write_report(report: dict, reports_dir: Path) -> Path:
    reports_dir.mkdir(parents=True, exist_ok=True)
    path = reports_dir / f"{report['pipeline_name']}_{report['execution_id'][:8]}.json"
    clean = {k: v for k, v in report.items() if k != "end_time_dt"}
    path.write_text(json.dumps(clean, indent=2, default=str), encoding="utf-8")
    return path
