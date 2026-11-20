import numpy as np
import pandas as pd
import pytest
from sqlalchemy import text

from src.config_loader import validate_pipeline_config
from src.exceptions import ReferenceDataError
from src.quality.validator import DataQualityValidator


def validator(qc, engine=None):
    cfg = validate_pipeline_config(
        {"name": "t", "source": {"type": "csv", "path": "x"}, "target": {"table": "t"}, "quality_checks": qc})
    return DataQualityValidator(cfg["quality_checks"], engine)


def by_name(result):
    return {r.check_name: r for r in result.results}


def test_check_result_contract_fields():
    df = pd.DataFrame({"id": [1, 2, None, 4]})
    res = validator({"required_columns": ["id"]}).validate(df)
    r = by_name(res)["null_check_required"]
    assert (r.total_checked, r.passed, r.failed, r.failure_pct) == (4, 3, 1, 25.0)
    assert r.error_details and "row 2" in r.error_details[0]
    assert set(r.to_dict()) >= {"check_name", "total_checked", "passed", "failed", "failure_pct", "error_details"}
    assert len(res.valid) == 3 and len(res.invalid) == 1 and "_dq_reasons" in res.invalid.columns


def test_valid_and_invalid_partition_is_complete_and_disjoint():
    df = pd.DataFrame({"id": [1, 1, 2, None], "v": ["a", "b", "c", "d"]})
    res = validator({"primary_key": "id"}).validate(df)
    assert len(res.valid) + len(res.invalid) == len(df)
    assert set(res.valid.index).isdisjoint(res.invalid.index)


def test_null_required_missing_column_is_gate_failure():
    res = validator({"required_columns": ["id", "email"]}).validate(pd.DataFrame({"id": [1]}))
    assert not res.gate_passed and "email" in res.gate_failures[0]
    assert by_name(res)["required_columns_present"].failed == 1


def test_primary_key_policies():
    df = pd.DataFrame({"id": [1, 1, 1, 2]})
    first = validator({"primary_key": "id", "primary_key_policy": "keep_first"}).validate(df)
    assert first.valid.index.tolist() == [0, 3]
    last = validator({"primary_key": "id", "primary_key_policy": "keep_last"}).validate(df)
    assert last.valid.index.tolist() == [2, 3]
    allq = validator({"primary_key": "id", "primary_key_policy": "quarantine_all"}).validate(df)
    assert allq.valid.index.tolist() == [3]


def test_composite_primary_key_and_null_key_rows():
    df = pd.DataFrame({"a": [1, 1, 1, None], "b": ["x", "x", "y", "z"]})
    res = validator({"primary_key": ["a", "b"]}).validate(df)
    assert res.valid.index.tolist() == [0, 2]            # idx1 duplicate key, idx3 null key part
    assert "null in required" in res.invalid.loc[3, "_dq_reasons"]


def test_duplicate_rows_check():
    df = pd.DataFrame({"a": [1, 1, 2], "b": ["x", "x", "y"]})
    res = validator({"check_duplicates": True}).validate(df)
    assert by_name(res)["duplicate_rows"].failed == 1 and len(res.valid) == 2


def test_data_type_validation_declared_types_and_conversion_failures():
    df = pd.DataFrame({"n": ["1", "x", "2.5"], "b": ["true", "no", "false"]})
    res = validator({"column_types": {"n": "integer", "b": "boolean"}}).validate(df, {0: ["column 'q': cannot convert"]})
    r = by_name(res)["data_type_validation"]
    assert r.failed == 3                                  # row0 conversion failure, row1 'x', row2 2.5 ; 'no' invalid boolean also row1
    assert "cannot convert" in res.invalid.loc[0, "_dq_reasons"]


def test_data_type_validation_other_types():
    df = pd.DataFrame({"f": ["1.5", "abc"], "d": ["2024-01-01", "nope"], "s": ["ok", 5]})
    res = validator({"column_types": {"f": "float", "d": "date", "s": "string"}}).validate(df)
    assert res.invalid.index.tolist() == [1]


def test_range_checks_with_bounds_and_non_numeric():
    df = pd.DataFrame({"age": [17, 18, 100, 101, None, "abc"]})
    res = validator({"range_checks": {"age": {"min": 18, "max": 100}}}).validate(df)
    assert res.invalid.index.tolist() == [0, 3, 5]
    assert by_name(res)["range_check:age"].failed == 3


def test_email_validation():
    df = pd.DataFrame({"e": ["a@b.com", "bad", "x@y", None, "ok.name+tag@sub.example.org"]})
    res = validator({"email_columns": ["e"]}).validate(df)
    assert res.invalid.index.tolist() == [1, 2]


def test_date_validation_range_and_today_keyword():
    df = pd.DataFrame({"d": pd.to_datetime(["2020-01-01", "2010-01-01", "2099-01-01", None])})
    res = validator({"date_checks": {"d": {"min": "2015-01-01", "max": "today"}}}).validate(df)
    assert res.invalid.index.tolist() == [1, 2]
    raw = pd.DataFrame({"d": ["2020-01-01", "garbage"]})
    res2 = validator({"date_checks": {"d": {"min": "2015-01-01"}}}).validate(raw)
    assert res2.invalid.index.tolist() == [1]


def test_null_threshold_gate_and_warn_mode():
    df = pd.DataFrame({"a": [1, None, None, 4], "b": [1, 2, 3, 4]})
    res = validator({"null_threshold": 0.25}).validate(df)
    assert not res.gate_passed and "column 'a'" in res.gate_failures[0]
    assert len(res.valid) == 4                             # dataset-level: rows are not quarantined
    ok = validator({"null_threshold": 0.25, "null_threshold_action": "warn"}).validate(df)
    assert ok.gate_passed and by_name(ok)["null_threshold"].status == "WARNING"
    within = validator({"null_threshold": 0.6, "null_threshold_columns": ["a"]}).validate(df)
    assert within.gate_passed


def test_custom_rule_pass_fail_and_error():
    df = pd.DataFrame({"q": [1, 2, 0], "p": [5.0, 5.0, 5.0]})
    rule = [{"name": "positive_total", "expression": "q * p > 0"}]
    res = validator({"custom_rules": rule}).validate(df)
    assert res.invalid.index.tolist() == [2]
    bad = validator({"custom_rules": [{"name": "broken", "expression": "nonexistent_col > 1"}]}).validate(df)
    assert not bad.gate_passed and "could not be evaluated" in bad.gate_failures[0]
    notbool = validator({"custom_rules": [{"name": "nb", "expression": "q + p"}]}).validate(df)
    assert not notbool.gate_passed


def test_check_on_missing_column_is_gate_failure_not_crash():
    res = validator({"range_checks": {"ghost": {"min": 0}}, "email_columns": ["ghost2"]}).validate(pd.DataFrame({"a": [1]}))
    assert not res.gate_passed and len(res.gate_failures) == 2


def test_referential_integrity(engine):
    with engine.begin() as c:
        c.execute(text("CREATE TABLE customers (customer_id INTEGER PRIMARY KEY)"))
        c.execute(text("INSERT INTO customers VALUES (1),(2)"))
    qc = {"referential_checks": [{"column": "cid", "reference_table": "customers", "reference_column": "customer_id"}]}
    df = pd.DataFrame({"cid": pd.array([1, 3, None, 2], dtype="Int64")})
    res = validator(qc, engine).validate(df)
    assert res.invalid.index.tolist() == [1]
    assert "not found in customers.customer_id" in res.invalid.loc[1, "_dq_reasons"]


def test_referential_missing_reference_table_raises(engine):
    qc = {"referential_checks": [{"column": "cid", "reference_table": "nope", "reference_column": "id"}]}
    with pytest.raises(ReferenceDataError, match="run the pipeline that loads it first"):
        validator(qc, engine).validate(pd.DataFrame({"cid": [1]}))


def test_empty_dataframe_validates_cleanly():
    df = pd.DataFrame({"id": pd.Series([], dtype=object)})
    res = validator({"primary_key": "id", "email_columns": ["id"], "null_threshold": 0.1}).validate(df)
    assert res.valid.empty and res.invalid.empty and res.gate_passed
    assert all(r.failure_pct == 0.0 for r in res.results)


def test_error_details_are_capped():
    df = pd.DataFrame({"id": [None] * 50})
    res = validator({"required_columns": ["id"], "max_error_details": 5}).validate(df)
    details = by_name(res)["null_check_required"].error_details
    assert len(details) == 6 and "45 more" in details[-1]
