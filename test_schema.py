import numpy as np
import pandas as pd
import pytest

from src.schema.drift_detector import detect_drift
from src.schema.schema_inference import infer_logical_type, infer_schema
from src.schema.schema_store import SchemaStore

POL = {"column_added": "exclude", "column_removed": "block", "type_changed": "block", "nullable_changed": "allow"}


def schema(**cols):
    """schema(a=('integer', False), ...) -> minimal schema doc."""
    return {"columns": {k: {"type": t, "nullable": n} for k, (t, n) in cols.items()}}


# ------------------------------------------------------------------------ inference
def test_infers_types_from_text_columns():
    df = pd.DataFrame({
        "i": ["1", "-2", "30"], "f": ["1.5", "2", "3e2"], "b": ["true", "False", "TRUE"],
        "d": ["2024-01-01", "01/02/2024", "2024-03-04 10:00:00"], "s": ["x", "1", "2024-01-01"],
        "zip": ["02134", "10001", "00501"], "mixed": ["1", "abc", "2"],
    })
    t = {c: v["type"] for c, v in infer_schema(df)["columns"].items()}
    assert t == {"i": "integer", "f": "float", "b": "boolean", "d": "datetime", "s": "string",
                 "zip": "string", "mixed": "string"}


def test_infers_types_from_typed_columns():
    df = pd.DataFrame({"i": [1, 2], "f": [1.5, 2.5], "b": [True, False],
                       "d": pd.to_datetime(["2024-01-01", "2024-01-02"]), "n": pd.array([1, None], dtype="Int64")})
    t = {c: v["type"] for c, v in infer_schema(df)["columns"].items()}
    assert t == {"i": "integer", "f": "float", "b": "boolean", "d": "datetime", "n": "integer"}


def test_invalid_date_looking_text_is_string():
    assert infer_logical_type(pd.Series(["2024-01-01", "31-31-2020"])) == "string"


def test_nullable_unique_stats_and_unknown():
    df = pd.DataFrame({"id": ["1", "2", "3"], "v": ["a", "a", None], "empty": [None, None, None]})
    cols = infer_schema(df)["columns"]
    assert cols["id"]["unique"] and not cols["id"]["nullable"]
    assert cols["id"]["stats"] == {"min": 1, "max": 3, "mean": 2.0}
    assert cols["v"]["nullable"] and cols["v"]["null_count"] == 1 and cols["v"]["null_pct"] == pytest.approx(0.3333, abs=1e-4)
    assert not cols["v"]["unique"] and cols["v"]["stats"]["max_length"] == 1
    assert cols["empty"]["type"] == "unknown"


def test_infer_schema_on_empty_frame_and_json_serialisable():
    import json
    s = infer_schema(pd.DataFrame({"a": pd.Series([], dtype=object)}))
    assert s["row_count"] == 0 and s["columns"]["a"]["type"] == "unknown"
    json.dumps(infer_schema(pd.DataFrame({"a": [1, 2], "b": ["x", None], "c": pd.to_datetime(["2024-01-01", None])})))


# ---------------------------------------------------------------------------- store
def test_schema_store_versions_and_drift_reports(tmp_path):
    store = SchemaStore(tmp_path, "p")
    assert store.latest() is None
    assert store.save({"columns": {}}, "e1") == 1
    assert store.save({"columns": {"a": {}}}, "e2") == 2
    version, doc = store.latest()
    assert version == 2 and doc["execution_id"] == "e2" and doc["schema_version"] == 2
    assert store.save_drift_report({"x": 1}, "abcdef123456").exists()


# ---------------------------------------------------------------------------- drift
def test_first_run_creates_baseline():
    r = detect_drift(None, schema(a=("integer", False)), POL)
    assert r.status == "BASELINE_CREATED" and r.decision == "proceed" and not r.has_changes


def test_no_change_is_none():
    s = schema(a=("integer", False), b=("string", True))
    r = detect_drift(s, s, POL)
    assert r.status == "NONE" and not r.changes and not r.blocked


def test_new_column_example_from_spec_is_detected_and_excluded():
    prev = schema(customer_id=("integer", False), customer_name=("string", False), email=("string", False))
    cur = schema(customer_id=("integer", False), customer_name=("string", False), email=("string", False),
                 phone=("string", True))
    r = detect_drift(prev, cur, POL)
    ch = r.changes[0]
    assert (ch.change_type, ch.column, ch.severity, ch.action) == ("column_added", "phone", "WARNING", "exclude")
    assert r.status == "WARNING" and r.decision == "proceed" and r.exclude_columns == ["phone"]


def test_new_column_add_and_block_policies():
    prev, cur = schema(a=("integer", False)), schema(a=("integer", False), b=("string", False))
    assert detect_drift(prev, cur, {**POL, "column_added": "add"}).add_columns == ["b"]
    blocked = detect_drift(prev, cur, {**POL, "column_added": "block"})
    assert blocked.blocked and "new column 'b'" in blocked.blocked_reasons[0]


def test_removed_column_is_critical_and_blocks_by_default():
    r = detect_drift(schema(a=("integer", False), b=("string", False)), schema(a=("integer", False)), POL)
    assert r.status == "CRITICAL" and r.blocked and r.changes[0].change_type == "column_removed"
    allowed = detect_drift(schema(a=("integer", False), b=("string", False)), schema(a=("integer", False)),
                           {**POL, "column_removed": "allow"})
    assert not allowed.blocked and allowed.status == "CRITICAL"   # still classified CRITICAL, but permitted


def test_type_change_classification():
    crit = detect_drift(schema(a=("integer", False)), schema(a=("string", False)), POL)
    assert crit.status == "CRITICAL" and crit.blocked
    widen = detect_drift(schema(a=("integer", False)), schema(a=("float", False)), POL)
    assert widen.status == "WARNING" and not widen.blocked
    narrow = detect_drift(schema(a=("float", False)), schema(a=("integer", False)), POL)
    assert narrow.status == "INFORMATIONAL" and not narrow.blocked
    ok = detect_drift(schema(a=("integer", False)), schema(a=("string", False)), {**POL, "type_changed": "allow"})
    assert not ok.blocked


def test_unknown_type_is_not_drift():
    assert detect_drift(schema(a=("unknown", False)), schema(a=("integer", False)), POL).status == "NONE"


def test_nullable_changes():
    to_null = detect_drift(schema(a=("integer", False)), schema(a=("integer", True)), POL)
    assert to_null.status == "WARNING" and not to_null.blocked
    strict = detect_drift(schema(a=("integer", False)), schema(a=("integer", True)), {**POL, "nullable_changed": "block"})
    assert strict.blocked
    to_notnull = detect_drift(schema(a=("integer", True)), schema(a=("integer", False)), POL)
    assert to_notnull.status == "INFORMATIONAL"


def test_accept_drift_overrides_blocks():
    prev = schema(a=("integer", False), gone=("string", False))
    cur = schema(a=("string", False))
    r = detect_drift(prev, cur, POL, accept_drift=True)
    assert not r.blocked and all(c.overridden for c in r.changes if c.severity == "CRITICAL")
    added = detect_drift(schema(a=("integer", False)), schema(a=("integer", False), n=("string", False)),
                         {**POL, "column_added": "block"}, accept_drift=True)
    assert added.add_columns == ["n"] and not added.blocked


def test_multiple_changes_sorted_by_severity_and_serialisable():
    import json
    prev = schema(a=("integer", False), b=("string", False))
    cur = schema(a=("float", False), c=("string", True))
    r = detect_drift(prev, cur, POL, previous_version=3)
    assert [c.severity for c in r.changes] == sorted([c.severity for c in r.changes],
                                                      key=["CRITICAL", "WARNING", "INFO"].index)
    d = r.to_dict()
    assert d["previous_version"] == 3 and d["summary"]["CRITICAL"] == 1
    json.dumps(d)
