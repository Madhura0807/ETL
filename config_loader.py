"""Load and validate settings + pipeline YAML. Validation collects ALL errors before raising."""
from __future__ import annotations

import copy
import re
from pathlib import Path

import yaml

from src.exceptions import ConfigError
from src.utils.paths import PROJECT_ROOT, resolve_path

SOURCE_TYPES = {"csv", "json", "api", "sql"}
COLUMN_TYPES = {"integer", "float", "string", "boolean", "date", "datetime"}
LOAD_MODES = {"full", "append", "upsert"}
ON_DUPLICATE = {"skip", "update", "error"}
PK_POLICIES = {"keep_first", "keep_last", "quarantine_all"}
IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")
NAME_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")

DEFAULT_SETTINGS = {
    "paths": {
        "schemas": "schemas", "reports": "reports", "logs": "logs",
        "quarantine": "data/quarantine", "processed": "data/processed",
    },
    "logging": {"level": "INFO", "console": True},
    "database": {"url_env": "ETL_TARGET_URL"},
    "quarantine": {"max_db_rows": 10000},
    "defaults": {
        "retry": {"max_attempts": 3, "backoff_seconds": 1.0, "backoff_factor": 2.0},
        "batch_size": 1000,
    },
}

DEFAULT_DRIFT_POLICIES = {
    "column_added": "exclude",     # exclude | add | block
    "column_removed": "block",     # block | allow
    "type_changed": "block",       # block | allow   (applies to incompatible changes)
    "nullable_changed": "allow",   # allow | block   (applies to non-null -> nullable)
}

TOP_KEYS = {"name", "description", "source", "target", "transformations", "quality_checks",
            "schema_drift", "load", "retry", "output"}
SOURCE_KEYS = {
    "csv": {"type", "path", "delimiter", "encoding"},
    "json": {"type", "path", "record_path", "flatten", "lines", "encoding"},
    "api": {"type", "url", "method", "headers", "params", "auth", "pagination", "data_key",
            "timeout", "max_pages"},
    "sql": {"type", "connection_env", "query", "params"},
}
TRANSFORM_KEYS = {"standardize_columns", "trim_strings", "remove_duplicates", "type_conversions",
                  "date_columns", "date_dayfirst", "missing_values", "normalize"}
QUALITY_KEYS = {"primary_key", "primary_key_policy", "required_columns", "null_threshold",
                "null_threshold_columns", "null_threshold_action", "check_duplicates",
                "column_types", "range_checks", "email_columns", "date_checks",
                "referential_checks", "custom_rules", "max_error_details"}
LOAD_KEYS = {"mode", "batch_size", "on_duplicate", "watermark_column"}


def deep_merge(base: dict, override: dict) -> dict:
    out = copy.deepcopy(base)
    for k, v in (override or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = copy.deepcopy(v)
    return out


def load_yaml(path: str | Path) -> dict:
    p = resolve_path(path)
    if not p.exists():
        raise ConfigError(f"config file not found: {p}")
    try:
        data = yaml.safe_load(p.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ConfigError(f"invalid YAML in {p}: {exc}") from exc
    if not isinstance(data, dict):
        raise ConfigError(f"{p} must contain a YAML mapping at the top level")
    return data


def load_settings(path: str | Path | None = "config/settings.yaml") -> dict:
    raw = load_yaml(path) if path and resolve_path(path).exists() else {}
    return deep_merge(DEFAULT_SETTINGS, raw)


def set_by_path(data: dict, dotted: str, value) -> None:
    keys = dotted.split(".")
    cur = data
    for k in keys[:-1]:
        cur = cur.setdefault(k, {})
    cur[keys[-1]] = value


def load_pipeline_config(path: str | Path, settings: dict | None = None,
                         overrides: dict | None = None) -> dict:
    """Read a pipeline YAML, apply ``--set`` overrides, validate and normalise it."""
    raw = load_yaml(path)
    if "pipeline" not in raw or not isinstance(raw["pipeline"], dict):
        raise ConfigError("top-level key 'pipeline' (a mapping) is required")
    pipeline = raw["pipeline"]
    for dotted, value in (overrides or {}).items():
        set_by_path(pipeline, dotted, value)
    return validate_pipeline_config(pipeline, settings or load_settings())


def _is_num(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _list_of_str(v) -> bool:
    return isinstance(v, list) and all(isinstance(i, str) for i in v)


def validate_pipeline_config(raw: dict, settings: dict | None = None) -> dict:
    """Return a normalised copy of ``raw`` with defaults applied, or raise ConfigError."""
    settings = settings or DEFAULT_SETTINGS
    errors: list[str] = []
    cfg = copy.deepcopy(raw)

    for k in cfg:
        if k not in TOP_KEYS:
            errors.append(f"unknown key 'pipeline.{k}' (allowed: {sorted(TOP_KEYS)})")

    name = cfg.get("name")
    if not isinstance(name, str) or not NAME_RE.match(name):
        errors.append("pipeline.name must match ^[a-z][a-z0-9_]*$")

    # ---- source -------------------------------------------------------
    src = cfg.get("source")
    if not isinstance(src, dict):
        errors.append("pipeline.source must be a mapping")
        src = {}
    stype = src.get("type")
    if stype not in SOURCE_TYPES:
        errors.append(f"pipeline.source.type must be one of {sorted(SOURCE_TYPES)}")
    else:
        for k in src:
            if k not in SOURCE_KEYS[stype]:
                errors.append(f"unknown key 'source.{k}' for source type '{stype}'")
        required = {"csv": ["path"], "json": ["path"], "api": ["url"], "sql": ["connection_env", "query"]}[stype]
        for k in required:
            if not isinstance(src.get(k), str) or not src.get(k).strip():
                errors.append(f"source.{k} is required for source type '{stype}'")
        if stype == "sql" and isinstance(src.get("query"), str):
            q = src["query"].strip().rstrip(";").strip()
            if not re.match(r"(?is)^(select|with)\b", q) or ";" in q:
                errors.append("source.query must be a single SELECT statement")
        if stype == "api":
            if "timeout" in src and not (_is_num(src["timeout"]) and src["timeout"] > 0):
                errors.append("source.timeout must be a positive number")
            pg = src.get("pagination") or {"type": "none"}
            if not isinstance(pg, dict) or pg.get("type", "none") not in {"none", "page", "offset", "link"}:
                errors.append("source.pagination.type must be one of none|page|offset|link")
            elif pg.get("type") == "link" and not pg.get("next_key"):
                errors.append("source.pagination.next_key is required for type 'link'")
            elif pg.get("type") in {"page", "offset"} and not (isinstance(pg.get("page_size"), int) and pg["page_size"] > 0):
                errors.append("source.pagination.page_size (positive int) is required for page/offset pagination")
            src.setdefault("method", "GET")
            src.setdefault("timeout", 10)
            src.setdefault("max_pages", 1000)
            src["pagination"] = pg
        if stype == "csv":
            src.setdefault("delimiter", ",")
            src.setdefault("encoding", "utf-8")
        if stype == "json":
            src.setdefault("flatten", True)
            src.setdefault("lines", False)
            src.setdefault("encoding", "utf-8")
        if stype == "sql":
            src.setdefault("params", {})
    cfg["source"] = src

    # ---- target -------------------------------------------------------
    tgt = cfg.setdefault("target", {})
    if not isinstance(tgt, dict):
        errors.append("pipeline.target must be a mapping")
        tgt = {}
    tgt.setdefault("type", "mysql")
    if tgt["type"] != "mysql":
        errors.append("pipeline.target.type must be 'mysql'")
    if not isinstance(tgt.get("table"), str) or not IDENT_RE.match(tgt.get("table", "")):
        errors.append("pipeline.target.table must be a valid identifier ([A-Za-z_][A-Za-z0-9_]*)")
    elif tgt["table"].startswith("etl_"):
        errors.append("target.table must not start with 'etl_' (reserved for pipeline metadata)")
    cfg["target"] = tgt

    # ---- transformations ----------------------------------------------
    tr = cfg.setdefault("transformations", {})
    if not isinstance(tr, dict):
        errors.append("pipeline.transformations must be a mapping")
        tr = {}
    for k in tr:
        if k not in TRANSFORM_KEYS:
            errors.append(f"unknown key 'transformations.{k}'")
    for flag in ("standardize_columns", "trim_strings", "date_dayfirst"):
        if flag in tr and not isinstance(tr[flag], bool):
            errors.append(f"transformations.{flag} must be true/false")
    rd = tr.get("remove_duplicates", False)
    if isinstance(rd, dict):
        if set(rd) - {"subset", "keep"}:
            errors.append("transformations.remove_duplicates only accepts 'subset' and 'keep'")
        if "subset" in rd and not _list_of_str(rd["subset"]):
            errors.append("transformations.remove_duplicates.subset must be a list of column names")
        if rd.get("keep", "first") not in {"first", "last"}:
            errors.append("transformations.remove_duplicates.keep must be first|last")
    elif not isinstance(rd, bool):
        errors.append("transformations.remove_duplicates must be true/false or a mapping")
    for k, v in (tr.get("type_conversions") or {}).items():
        if v not in COLUMN_TYPES:
            errors.append(f"transformations.type_conversions.{k}: '{v}' not in {sorted(COLUMN_TYPES)}")
    if "type_conversions" in tr and not isinstance(tr["type_conversions"], dict):
        errors.append("transformations.type_conversions must be a mapping column -> type")
    if "date_columns" in tr and not _list_of_str(tr["date_columns"]):
        errors.append("transformations.date_columns must be a list of column names")
    mv = tr.get("missing_values", {})
    if not isinstance(mv, dict) or set(mv) - {"fill", "strategies"}:
        errors.append("transformations.missing_values accepts only 'fill' and 'strategies' mappings")
    else:
        for col, strat in (mv.get("strategies") or {}).items():
            if strat not in {"mean", "median", "mode"}:
                errors.append(f"missing_values.strategies.{col}: must be mean|median|mode")
    nm = tr.get("normalize", {})
    if not isinstance(nm, dict) or set(nm) - {"lowercase", "uppercase", "titlecase"}:
        errors.append("transformations.normalize accepts only lowercase/uppercase/titlecase lists")
    else:
        for k, v in nm.items():
            if not _list_of_str(v):
                errors.append(f"transformations.normalize.{k} must be a list of column names")
    cfg["transformations"] = tr

    # ---- quality checks -----------------------------------------------
    qc = cfg.setdefault("quality_checks", {})
    if not isinstance(qc, dict):
        errors.append("pipeline.quality_checks must be a mapping")
        qc = {}
    for k in qc:
        if k not in QUALITY_KEYS:
            errors.append(f"unknown key 'quality_checks.{k}'")
    pk = qc.get("primary_key", [])
    if isinstance(pk, str):
        pk = [pk]
    if not _list_of_str(pk):
        errors.append("quality_checks.primary_key must be a column name or list of names")
        pk = []
    qc["primary_key"] = pk
    qc.setdefault("primary_key_policy", "keep_first")
    if qc["primary_key_policy"] not in PK_POLICIES:
        errors.append(f"quality_checks.primary_key_policy must be one of {sorted(PK_POLICIES)}")
    qc.setdefault("required_columns", [])
    if not _list_of_str(qc["required_columns"]):
        errors.append("quality_checks.required_columns must be a list of column names")
    qc.setdefault("null_threshold", 1.0)
    if not (_is_num(qc["null_threshold"]) and 0 <= qc["null_threshold"] <= 1):
        errors.append("quality_checks.null_threshold must be a number between 0 and 1")
    qc.setdefault("null_threshold_columns", [])
    if not _list_of_str(qc["null_threshold_columns"]):
        errors.append("quality_checks.null_threshold_columns must be a list of column names")
    qc.setdefault("null_threshold_action", "fail")
    if qc["null_threshold_action"] not in {"fail", "warn"}:
        errors.append("quality_checks.null_threshold_action must be fail|warn")
    qc.setdefault("check_duplicates", False)
    qc.setdefault("max_error_details", 20)
    for col, t in (qc.get("column_types") or {}).items():
        if t not in COLUMN_TYPES:
            errors.append(f"quality_checks.column_types.{col}: '{t}' not in {sorted(COLUMN_TYPES)}")
    qc.setdefault("column_types", {})
    for section in ("range_checks", "date_checks"):
        qc.setdefault(section, {})
        if not isinstance(qc[section], dict):
            errors.append(f"quality_checks.{section} must be a mapping column -> {{min, max}}")
            continue
        for col, rule in qc[section].items():
            if not isinstance(rule, dict) or set(rule) - {"min", "max"} or not rule:
                errors.append(f"quality_checks.{section}.{col} needs 'min' and/or 'max'")
            elif section == "range_checks":
                for b in rule.values():
                    if not _is_num(b):
                        errors.append(f"quality_checks.range_checks.{col}: bounds must be numbers")
                if "min" in rule and "max" in rule and _is_num(rule["min"]) and _is_num(rule["max"]) and rule["min"] > rule["max"]:
                    errors.append(f"quality_checks.range_checks.{col}: min > max")
    qc.setdefault("email_columns", [])
    if not _list_of_str(qc["email_columns"]):
        errors.append("quality_checks.email_columns must be a list of column names")
    qc.setdefault("referential_checks", [])
    refs = qc["referential_checks"]
    if not isinstance(refs, list):
        errors.append("quality_checks.referential_checks must be a list")
    else:
        for i, r in enumerate(refs):
            if not isinstance(r, dict) or set(r) != {"column", "reference_table", "reference_column"} \
                    or not all(isinstance(v, str) and v for v in r.values()):
                errors.append(f"quality_checks.referential_checks[{i}] needs column, reference_table, reference_column")
            elif not IDENT_RE.match(r["reference_table"]):
                errors.append(f"quality_checks.referential_checks[{i}].reference_table is not a valid identifier")
    qc.setdefault("custom_rules", [])
    rules = qc["custom_rules"]
    if not isinstance(rules, list):
        errors.append("quality_checks.custom_rules must be a list")
    else:
        for i, r in enumerate(rules):
            if not isinstance(r, dict) or set(r) != {"name", "expression"} \
                    or not all(isinstance(v, str) and v.strip() for v in r.values()):
                errors.append(f"quality_checks.custom_rules[{i}] needs 'name' and 'expression' strings")
    cfg["quality_checks"] = qc

    # ---- schema drift ---------------------------------------------------
    sd = cfg.setdefault("schema_drift", {})
    if not isinstance(sd, dict) or set(sd) - {"policies"}:
        errors.append("pipeline.schema_drift only accepts 'policies'")
        sd = {}
    policies = {**DEFAULT_DRIFT_POLICIES, **(sd.get("policies") or {})}
    allowed = {"column_added": {"exclude", "add", "block"}, "column_removed": {"block", "allow"},
               "type_changed": {"block", "allow"}, "nullable_changed": {"allow", "block"}}
    for k, v in policies.items():
        if k not in allowed:
            errors.append(f"unknown schema_drift policy '{k}'")
        elif v not in allowed[k]:
            errors.append(f"schema_drift.policies.{k} must be one of {sorted(allowed[k])}")
    cfg["schema_drift"] = {"policies": policies}

    # ---- load -----------------------------------------------------------
    ld = cfg.setdefault("load", {})
    if not isinstance(ld, dict):
        errors.append("pipeline.load must be a mapping")
        ld = {}
    for k in ld:
        if k not in LOAD_KEYS:
            errors.append(f"unknown key 'load.{k}'")
    ld.setdefault("mode", "append")
    ld.setdefault("batch_size", settings["defaults"]["batch_size"])
    if ld["mode"] not in LOAD_MODES:
        errors.append(f"load.mode must be one of {sorted(LOAD_MODES)}")
    if not (isinstance(ld["batch_size"], int) and not isinstance(ld["batch_size"], bool) and ld["batch_size"] > 0):
        errors.append("load.batch_size must be a positive integer")
    ld.setdefault("on_duplicate", "update" if ld["mode"] == "upsert" else "skip")
    if ld["on_duplicate"] not in ON_DUPLICATE:
        errors.append(f"load.on_duplicate must be one of {sorted(ON_DUPLICATE)}")
    if ld["mode"] == "upsert" and not pk:
        errors.append("load.mode 'upsert' requires quality_checks.primary_key")
    if ld["mode"] == "upsert" and ld["on_duplicate"] != "update":
        errors.append("load.mode 'upsert' implies on_duplicate: update")
    if ld.get("watermark_column"):
        if not isinstance(ld["watermark_column"], str):
            errors.append("load.watermark_column must be a column name")
        if ld["mode"] == "full":
            errors.append("incremental loading (load.watermark_column) cannot be combined with mode 'full'")
        if ld["mode"] == "append" and ld["on_duplicate"] == "error":
            errors.append("incremental append needs on_duplicate skip|update so re-reading boundary rows is safe")
        if not pk:
            errors.append("incremental loading requires quality_checks.primary_key so re-reads are idempotent")
    cfg["load"] = ld

    # ---- retry / output ---------------------------------------------------
    retry = deep_merge(settings["defaults"]["retry"], cfg.get("retry") or {})
    if not (isinstance(retry["max_attempts"], int) and retry["max_attempts"] >= 1):
        errors.append("retry.max_attempts must be an integer >= 1")
    for k in ("backoff_seconds", "backoff_factor"):
        if not (_is_num(retry[k]) and retry[k] >= 0):
            errors.append(f"retry.{k} must be a non-negative number")
    cfg["retry"] = retry
    out = {"save_processed": True, **(cfg.get("output") or {})}
    if not isinstance(out["save_processed"], bool):
        errors.append("output.save_processed must be true/false")
    cfg["output"] = out

    if errors:
        raise ConfigError(errors)
    return cfg


def settings_paths(settings: dict) -> dict[str, Path]:
    """Absolute directories from settings (relative -> project root)."""
    return {k: resolve_path(v) for k, v in settings["paths"].items()}
