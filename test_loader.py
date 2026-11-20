import pandas as pd
import pytest
from sqlalchemy import exc as sa_exc
from sqlalchemy import inspect, text

from src.db.repository import MetadataRepository
from src.exceptions import SchemaMismatchError
from src.loaders.mysql_loader import MySQLLoader, to_records
from src.utils.retry import is_transient_db_error, retry_call

RETRY = {"max_attempts": 3, "backoff_seconds": 0, "backoff_factor": 1}


def make_loader(engine, mode="upsert", batch_size=2, pk=("id",), on_duplicate=None, repo=None, **kw):
    on_duplicate = on_duplicate or ("update" if mode == "upsert" else "skip")
    return MySQLLoader(engine, "people", mode, batch_size, list(pk), on_duplicate, RETRY, repo, "p",
                       sleep=lambda s: None, **kw)


def rows(engine, sql="SELECT * FROM people ORDER BY id"):
    with engine.connect() as c:
        return [tuple(r) for r in c.execute(text(sql))]


def df(n=5, start=1):
    return pd.DataFrame({"id": range(start, start + n), "name": [f"n{i}" for i in range(start, start + n)]})


# ------------------------------------------------------------------ retry helper
def test_retry_call_behaviour():
    calls = []

    def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise ValueError("boom")
        return "ok"

    assert retry_call(flaky, max_attempts=3, backoff_seconds=0, sleep=lambda s: None) == "ok"
    with pytest.raises(ValueError):
        retry_call(lambda: (_ for _ in ()).throw(ValueError("x")), max_attempts=2, sleep=lambda s: None)
    n = []
    with pytest.raises(KeyError):
        retry_call(lambda: n.append(1) or (_ for _ in ()).throw(KeyError("k")), max_attempts=5,
                   should_retry=lambda e: False, sleep=lambda s: None)
    assert len(n) == 1


def test_transient_error_classification():
    assert is_transient_db_error(sa_exc.OperationalError("s", {}, Exception("conn lost")))
    assert not is_transient_db_error(sa_exc.IntegrityError("s", {}, Exception("dup")))
    assert not is_transient_db_error(ValueError("x"))


# ------------------------------------------------------------------------ loading
def test_creates_table_with_types_and_loads_in_batches(engine):
    frame = pd.DataFrame({"id": pd.array([1, 2, 3], dtype="Int64"), "price": [1.5, None, 3.0],
                          "d": pd.to_datetime(["2024-01-01", None, "2024-01-03"]), "ok": [True, False, True],
                          "name": ["a", None, "c"]})
    res = make_loader(engine, date_columns={"d"}).load(frame, "exec-1")
    assert res.loaded_count == 3 and len(res.batches) == 2 and res.failed_batch_count == 0
    cols = {c["name"]: str(c["type"]) for c in inspect(engine).get_columns("people")}
    assert cols["id"] == "BIGINT" and cols["d"] == "DATE" and cols["price"] == "FLOAT"
    assert inspect(engine).get_pk_constraint("people")["constrained_columns"] == ["id"]
    out = rows(engine)
    assert out[1][1] is None and out[1][2] is None            # NaN / NaT stored as NULL


def test_upsert_updates_existing_and_is_idempotent(engine):
    loader = make_loader(engine)
    loader.load(df(3), "e1")
    changed = df(3)
    changed.loc[0, "name"] = "CHANGED"
    loader.load(pd.concat([changed, df(2, start=4)]), "e2")
    loader.load(pd.concat([changed, df(2, start=4)]), "e3")          # a retried run: no duplicates
    out = rows(engine)
    assert len(out) == 5 and out[0] == (1, "CHANGED")


def test_append_skip_keeps_existing_rows_and_adds_new(engine):
    loader = make_loader(engine, mode="append")
    loader.load(df(3), "e1")
    again = df(4)
    again["name"] = "OVERWRITE?"
    res = loader.load(again, "e2")
    assert res.failed_batch_count == 0
    out = rows(engine)
    assert len(out) == 4 and out[0][1] == "n1" and out[3][1] == "OVERWRITE?"


def test_append_error_mode_fails_batch_on_duplicate(engine):
    loader = make_loader(engine, mode="append", on_duplicate="error", batch_size=10)
    loader.load(df(2), "e1")
    res = loader.load(df(2), "e2")
    assert res.failed_batch_count == 1 and res.loaded_count == 0 and len(res.failed_rows) == 2


def test_partial_load_keeps_good_batches_and_returns_failed_rows(engine):
    with engine.begin() as c:
        c.execute(text("CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"))
    frame = df(6)
    frame.loc[2, "name"] = None            # lands in batch 2 (rows idx 2,3) -> NOT NULL violation
    res = make_loader(engine).load(frame, "e1")
    assert [b.status for b in res.batches] == ["SUCCESS", "FAILED", "SUCCESS"]
    assert res.loaded_count == 4 and res.failed_batch_count == 1
    assert res.failed_rows["id"].tolist() == [3, 4] and "IntegrityError" in res.failed_rows["_load_error"].iloc[0]
    assert [r[0] for r in rows(engine)] == [1, 2, 5, 6]
    assert res.batches[1].attempts == 1     # integrity errors are NOT retried


def test_transient_failure_is_retried_and_succeeds_without_duplicates(engine, monkeypatch):
    loader = make_loader(engine, batch_size=10)
    real = MySQLLoader._execute_batch
    state = {"n": 0}

    def flaky(self, *a, **k):
        state["n"] += 1
        if state["n"] < 3:
            raise sa_exc.OperationalError("INSERT", {}, Exception("connection lost"))
        return real(self, *a, **k)

    monkeypatch.setattr(MySQLLoader, "_execute_batch", flaky)
    res = loader.load(df(4), "e1")
    assert res.batches[0].status == "SUCCESS" and res.batches[0].attempts == 3
    assert len(rows(engine)) == 4


def test_transient_failure_exhausts_retries_and_marks_batch_failed(engine, monkeypatch):
    def always(self, *a, **k):
        raise sa_exc.OperationalError("INSERT", {}, Exception("database down"))

    monkeypatch.setattr(MySQLLoader, "_execute_batch", always)
    res = make_loader(engine, batch_size=10).load(df(3), "e1")
    assert res.failed_batch_count == 1 and res.batches[0].attempts == 3 and res.loaded_count == 0
    assert len(res.failed_rows) == 3


def test_batch_log_is_written_atomically_with_data(engine):
    repo = MetadataRepository(engine)
    repo.create_tables()
    repo.start_run("e1", "p", "csv", __import__("datetime").datetime(2024, 1, 1))
    make_loader(engine, repo=repo).load(df(5), "e1")
    log = rows(engine, "SELECT batch_number, row_count, status FROM etl_batch_log ORDER BY batch_number")
    assert log == [(1, 2, "SUCCESS"), (2, 2, "SUCCESS"), (3, 1, "SUCCESS")]


def test_full_load_replaces_data_atomically(engine):
    loader = make_loader(engine, mode="full")
    loader.load(df(5), "aaaaaaaa-1")
    res = loader.load(df(2, start=100), "bbbbbbbb-2")
    assert res.loaded_count == 2 and [r[0] for r in rows(engine)] == [100, 101]
    assert not [t for t in inspect(engine).get_table_names() if "__stg_" in t]   # staging cleaned up


def test_full_load_failure_leaves_existing_data_untouched(engine):
    with engine.begin() as c:
        c.execute(text("CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"))
        c.execute(text("INSERT INTO people VALUES (1,'old1'),(2,'old2')"))
    bad = df(4, start=10)
    bad.loc[3, "name"] = None
    res = make_loader(engine, mode="full").load(bad, "cccccccc-3")
    assert res.aborted and res.loaded_count == 0 and res.failed_batch_count == 1
    assert rows(engine) == [(1, "old1"), (2, "old2")]
    assert not [t for t in inspect(engine).get_table_names() if "__stg_" in t]


def test_unexpected_columns_raise_unless_approved_then_alter(engine):
    loader = make_loader(engine)
    loader.load(df(2), "e1")
    wider = df(2)
    wider["phone"] = ["1", "2"]
    with pytest.raises(SchemaMismatchError, match="phone"):
        loader.load(wider, "e2")
    loader.load(wider, "e3", add_columns={"phone"})
    assert "phone" in [c["name"] for c in inspect(engine).get_columns("people")]
    assert rows(engine, "SELECT phone FROM people ORDER BY id") == [("1",), ("2",)]


def test_primary_key_mismatch_with_existing_table_is_rejected(engine):
    with engine.begin() as c:
        c.execute(text("CREATE TABLE people (id INTEGER, name TEXT)"))   # no PK -> upsert unreliable
    with pytest.raises(SchemaMismatchError, match="primary key"):
        make_loader(engine).load(df(2), "e1")


def test_empty_dataframe_loads_nothing(engine):
    res = make_loader(engine).load(df(0), "e1")
    assert res.loaded_count == 0 and res.batches == []


def test_to_records_converts_missing_and_numpy_values(engine):
    frame = pd.DataFrame({"id": pd.array([1, None], dtype="Int64"), "f": [1.5, float("nan")],
                          "d": pd.to_datetime(["2024-05-01", None])})
    make_loader(engine, date_columns={"d"}).load(frame.dropna(subset=["id"]), "e1")
    from sqlalchemy import MetaData, Table
    table = Table("people", MetaData(), autoload_with=engine)
    recs = to_records(frame, table)
    assert recs[1]["id"] is None and recs[1]["f"] is None and recs[1]["d"] is None
    assert type(recs[0]["id"]) is int and recs[0]["d"].__class__.__name__ == "date"
