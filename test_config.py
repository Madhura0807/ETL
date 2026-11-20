import pytest

from src.config_loader import load_pipeline_config, load_settings, validate_pipeline_config
from src.exceptions import ConfigError
from src.main import main, parse_overrides

BASE = {"name": "p1", "source": {"type": "csv", "path": "a.csv"}, "target": {"table": "t"},
        "quality_checks": {"primary_key": "id"}}


def errors_of(**changes):
    raw = {**BASE, **changes}
    with pytest.raises(ConfigError) as exc:
        validate_pipeline_config(raw)
    return exc.value.errors


def test_valid_config_gets_defaults():
    cfg = validate_pipeline_config(BASE)
    assert cfg["quality_checks"]["primary_key"] == ["id"]
    assert cfg["load"]["mode"] == "append" and cfg["load"]["on_duplicate"] == "skip"
    assert cfg["schema_drift"]["policies"]["column_removed"] == "block"
    assert cfg["retry"]["max_attempts"] == 3


@pytest.mark.parametrize("name", ["customers", "orders", "products"])
def test_shipped_pipeline_configs_are_valid(name):
    cfg = load_pipeline_config(f"config/pipelines/{name}.yaml", load_settings())
    assert cfg["target"]["table"] == name


def test_unknown_keys_are_rejected():
    assert any("unknown key 'pipeline.bogus'" in e for e in errors_of(bogus=1))
    assert any("transformations.nope" in e for e in errors_of(transformations={"nope": True}))


def test_collects_multiple_errors_at_once():
    errs = errors_of(name="Bad Name", source={"type": "ftp"}, load={"mode": "merge", "batch_size": 0})
    assert len(errs) >= 4


def test_missing_required_source_fields():
    assert any("source.path is required" in e for e in errors_of(source={"type": "csv"}))
    assert any("source.url is required" in e for e in errors_of(source={"type": "api"}))
    assert any("connection_env" in e for e in errors_of(source={"type": "sql", "query": "select 1"}))


def test_sql_source_must_be_single_select():
    src = {"type": "sql", "connection_env": "X", "query": "DROP TABLE users"}
    assert any("single SELECT" in e for e in errors_of(source=src))
    src["query"] = "SELECT 1; DELETE FROM users"
    assert any("single SELECT" in e for e in errors_of(source=src))


def test_upsert_requires_primary_key():
    errs = errors_of(quality_checks={}, load={"mode": "upsert"})
    assert any("requires quality_checks.primary_key" in e for e in errs)


def test_incremental_rules():
    assert any("cannot be combined with mode 'full'" in e
               for e in errors_of(load={"mode": "full", "watermark_column": "d"}))


def test_threshold_and_table_name_validation():
    assert any("null_threshold" in e for e in errors_of(quality_checks={"null_threshold": 1.5}))
    assert any("valid identifier" in e for e in errors_of(target={"table": "bad-name"}))
    assert any("reserved" in e for e in errors_of(target={"table": "etl_runs"}))


def test_range_min_greater_than_max():
    assert any("min > max" in e for e in errors_of(quality_checks={"range_checks": {"a": {"min": 5, "max": 1}}}))


def test_drift_policy_values_validated():
    assert any("column_removed" in e for e in errors_of(schema_drift={"policies": {"column_removed": "maybe"}}))


def test_missing_file_and_bad_yaml(tmp_path):
    with pytest.raises(ConfigError):
        load_pipeline_config(tmp_path / "nope.yaml", load_settings())
    bad = tmp_path / "bad.yaml"
    bad.write_text("pipeline: [1, 2")
    with pytest.raises(ConfigError):
        load_pipeline_config(bad, load_settings())
    notmap = tmp_path / "list.yaml"
    notmap.write_text("- a\n- b\n")
    with pytest.raises(ConfigError):
        load_pipeline_config(notmap, load_settings())


def test_cli_validate_config_and_overrides(capsys):
    assert main(["--pipeline", "config/pipelines/customers.yaml", "--validate-config"]) == 0
    assert parse_overrides(["source.path=x.csv"]) == {"source.path": "x.csv"}
    with pytest.raises(ConfigError):
        parse_overrides(["nokeyvalue"])
    assert main(["--pipeline", "config/pipelines/customers.yaml", "--validate-config",
                 "--set", "load.mode=bogus"]) == 1
