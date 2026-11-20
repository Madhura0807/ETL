"""CLI entry point.

    python -m src.main --pipeline config/pipelines/customers.yaml
    python -m src.main --all
    python -m src.main --pipeline config/pipelines/customers.yaml --validate-config
    python -m src.main --pipeline ... --set source.path=data/raw/customers_drift_added.csv --accept-drift

Exit codes: 0 SUCCESS | 1 FAILED/config error | 2 PARTIAL_SUCCESS | 3 BLOCKED
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from src.config_loader import load_pipeline_config, load_settings
from src.exceptions import ConfigError
from src.pipeline.orchestrator import PipelineOrchestrator
from src.utils.env import load_dotenv_file
from src.utils.paths import PROJECT_ROOT, resolve_path

EXIT_CODES = {"SUCCESS": 0, "FAILED": 1, "PARTIAL_SUCCESS": 2, "BLOCKED": 3}


def parse_overrides(pairs: list[str]) -> dict:
    out = {}
    for pair in pairs or []:
        if "=" not in pair:
            raise ConfigError(f"--set expects key=value, got '{pair}'")
        key, _, value = pair.partition("=")
        out[key.strip()] = value
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Configurable ETL framework")
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument("--pipeline", help="path to a pipeline YAML")
    group.add_argument("--all", action="store_true", help="run every YAML in config/pipelines (alphabetical)")
    ap.add_argument("--settings", default="config/settings.yaml")
    ap.add_argument("--validate-config", action="store_true", help="validate configuration and exit")
    ap.add_argument("--accept-drift", action="store_true", help="accept a reviewed, otherwise blocking schema change")
    ap.add_argument("--set", action="append", metavar="KEY=VALUE", help="override a pipeline setting (dotted path)")
    args = ap.parse_args(argv)

    load_dotenv_file(PROJECT_ROOT / ".env")
    try:
        settings = load_settings(args.settings)
        overrides = parse_overrides(args.set)
        paths = (sorted(resolve_path("config/pipelines").glob("*.yaml")) if args.all else [Path(args.pipeline)])
        configs = [load_pipeline_config(p, settings, overrides) for p in paths]
    except ConfigError as exc:
        print("Configuration invalid:", file=sys.stderr)
        for e in exc.errors:
            print(f"  - {e}", file=sys.stderr)
        return 1
    if args.validate_config:
        print(f"OK: {len(configs)} pipeline configuration(s) valid: {[c['name'] for c in configs]}")
        return 0

    worst = 0
    for cfg in configs:
        report = PipelineOrchestrator(cfg, settings, accept_drift=args.accept_drift).run()
        print(json.dumps({k: report[k] for k in ("pipeline_name", "execution_id", "status", "counts",
                                                 "schema_drift_status", "error")}, indent=2, default=str))
        print(f"report: {report['artifacts'].get('report_file')}")
        code = EXIT_CODES.get(report["status"], 1)
        worst = 1 if (worst == 1 or code == 1) else max(worst, code)  # FAILED outranks everything
    return worst


if __name__ == "__main__":
    sys.exit(main())
