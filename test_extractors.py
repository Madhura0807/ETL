import json
import sqlite3

import pytest
import requests

from src.exceptions import ExtractionError
from src.extractors import build_extractor
from src.extractors.api_extractor import APIExtractor

RETRY = {"max_attempts": 3, "backoff_seconds": 0, "backoff_factor": 1}
NOSLEEP = lambda s: None  # noqa: E731


def extract(cfg, **kw):
    return build_extractor(cfg, RETRY, sleep=NOSLEEP).extract(**kw)


# ----------------------------------------------------------------------------- CSV
def csv_cfg(path, **kw):
    return {"type": "csv", "path": str(path), **kw}


def test_csv_reads_everything_as_text_and_nulls(tmp_path):
    p = tmp_path / "a.csv"
    p.write_text("id,name,score\n1,Ann,3.5\n2,,\n")
    res = extract(csv_cfg(p))
    assert list(res.data.columns) == ["id", "name", "score"]
    assert res.data["id"].tolist() == ["1", "2"]
    assert res.data["name"].isna().tolist() == [False, True]
    assert res.metadata["row_count"] == 2 and "extracted_at" in res.metadata


def test_csv_malformed_rows_are_rejected_not_lost(tmp_path):
    p = tmp_path / "a.csv"
    p.write_text("id,name\n1,Ann\n2,Bob,extra,fields\n3,Cy\n")
    res = extract(csv_cfg(p))
    assert len(res.data) == 2
    assert len(res.rejected) == 1 and "2,Bob,extra,fields" in res.rejected[0]["record"]


def test_csv_empty_file_and_header_only(tmp_path):
    empty = tmp_path / "e.csv"
    empty.write_text("")
    with pytest.raises(ExtractionError):
        extract(csv_cfg(empty))
    header = tmp_path / "h.csv"
    header.write_text("id,name\n")
    res = extract(csv_cfg(header))
    assert res.data.empty and list(res.data.columns) == ["id", "name"]


def test_csv_missing_file(tmp_path):
    with pytest.raises(ExtractionError):
        extract(csv_cfg(tmp_path / "missing.csv"))


def test_csv_custom_delimiter(tmp_path):
    p = tmp_path / "a.csv"
    p.write_text("id;name\n1;Ann\n")
    assert extract(csv_cfg(p, delimiter=";")).data["name"].tolist() == ["Ann"]


# ---------------------------------------------------------------------------- JSON
def json_cfg(path, **kw):
    return {"type": "json", "path": str(path), "flatten": True, **kw}


def test_json_flattens_nested_and_handles_inconsistent_fields(tmp_path):
    p = tmp_path / "o.json"
    p.write_text(json.dumps({"orders": [
        {"id": 1, "ship": {"city": "Pune"}, "tags": ["a", "b"]},
        {"id": 2, "ship": {"city": "Goa", "zip": "403"}},
    ]}))
    df = extract(json_cfg(p, record_path="orders")).data
    assert {"id", "ship_city", "ship_zip", "tags"} <= set(df.columns)
    assert df["ship_zip"].isna().tolist() == [True, False]
    assert df.loc[0, "tags"] == '["a", "b"]'


def test_json_top_level_list_and_single_object(tmp_path):
    p = tmp_path / "l.json"
    p.write_text('[{"a": 1}, {"a": 2}]')
    assert len(extract(json_cfg(p)).data) == 2
    p.write_text('{"a": 1}')
    assert len(extract(json_cfg(p)).data) == 1


def test_json_malformed_raises(tmp_path):
    p = tmp_path / "bad.json"
    p.write_text('{"orders": [ {"id": 1}, ')
    with pytest.raises(ExtractionError, match="malformed JSON"):
        extract(json_cfg(p))


def test_json_missing_record_path(tmp_path):
    p = tmp_path / "o.json"
    p.write_text('{"x": []}')
    with pytest.raises(ExtractionError, match="record_path"):
        extract(json_cfg(p, record_path="orders"))


def test_json_non_object_records_rejected(tmp_path):
    p = tmp_path / "o.json"
    p.write_text('[{"a": 1}, 5, "text"]')
    res = extract(json_cfg(p))
    assert len(res.data) == 1 and len(res.rejected) == 2


def test_json_lines_with_bad_line(tmp_path):
    p = tmp_path / "o.jsonl"
    p.write_text('{"a": 1}\nnot json\n\n{"a": 3}\n')
    res = extract(json_cfg(p, lines=True))
    assert res.data["a"].tolist() == [1, 3] and len(res.rejected) == 1


def test_json_empty_list_gives_empty_frame(tmp_path):
    p = tmp_path / "o.json"
    p.write_text("[]")
    assert extract(json_cfg(p)).data.empty


# ----------------------------------------------------------------------------- API
class FakeResponse:
    def __init__(self, payload=None, status=200, bad_json=False):
        self.payload, self.status_code, self.bad_json = payload, status, bad_json

    def json(self):
        if self.bad_json:
            raise ValueError("no json")
        return self.payload


class FakeSession:
    def __init__(self, script):
        self.script, self.calls = list(script), []

    def request(self, method, url, headers=None, params=None, timeout=None):
        self.calls.append({"url": url, "params": dict(params or {}), "headers": headers, "timeout": timeout})
        item = self.script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def api_extract(monkeypatch, script, **cfg):
    session = FakeSession(script)
    monkeypatch.setattr(APIExtractor, "_session", lambda self: session)
    config = {"type": "api", "url": "https://api.test/items", "timeout": 5, "max_pages": 1000,
              "pagination": {"type": "none"}, **cfg}
    return extract(config), session


def test_api_single_page_with_data_key(monkeypatch):
    res, s = api_extract(monkeypatch, [FakeResponse({"data": [{"id": 1}, {"id": 2}]})], data_key="data")
    assert len(res.data) == 2 and s.calls[0]["timeout"] == 5


def test_api_page_pagination_stops_on_short_page(monkeypatch):
    script = [FakeResponse([{"id": 1}, {"id": 2}]), FakeResponse([{"id": 3}])]
    res, s = api_extract(monkeypatch, script, pagination={"type": "page", "page_size": 2})
    assert res.data["id"].tolist() == [1, 2, 3]
    assert [c["params"]["page"] for c in s.calls] == [1, 2]


def test_api_offset_and_link_pagination(monkeypatch):
    res, s = api_extract(monkeypatch, [FakeResponse([{"id": 1}]), FakeResponse([])],
                         pagination={"type": "offset", "page_size": 1})
    assert [c["params"]["offset"] for c in s.calls] == [0, 1]
    script = [FakeResponse({"items": [{"id": 1}], "next": "https://api.test/p2"}),
              FakeResponse({"items": [{"id": 2}], "next": None})]
    res, s = api_extract(monkeypatch, script, data_key="items", pagination={"type": "link", "next_key": "next"})
    assert res.data["id"].tolist() == [1, 2] and s.calls[1]["url"] == "https://api.test/p2"


def test_api_timeout_is_retried_then_succeeds(monkeypatch):
    res, s = api_extract(monkeypatch, [requests.Timeout("slow"), requests.ConnectionError("x"),
                                       FakeResponse([{"id": 1}])])
    assert len(res.data) == 1 and len(s.calls) == 3


def test_api_timeout_exhausted_raises_extraction_error(monkeypatch):
    with pytest.raises(ExtractionError, match="after 3 attempt"):
        api_extract(monkeypatch, [requests.Timeout("slow")] * 3)


def test_api_500_retried_but_404_is_not(monkeypatch):
    res, s = api_extract(monkeypatch, [FakeResponse(status=503), FakeResponse([{"id": 1}])])
    assert len(s.calls) == 2
    with pytest.raises(ExtractionError, match="not retryable"):
        api_extract(monkeypatch, [FakeResponse(status=404)])


def test_api_response_validation(monkeypatch):
    with pytest.raises(ExtractionError, match="not valid JSON"):
        api_extract(monkeypatch, [FakeResponse(bad_json=True)])
    with pytest.raises(ExtractionError, match="not a list"):
        api_extract(monkeypatch, [FakeResponse({"oops": 1})])
    with pytest.raises(ExtractionError, match="data_key"):
        api_extract(monkeypatch, [FakeResponse({"oops": 1})], data_key="data")


def test_api_refuses_to_truncate_silently(monkeypatch):
    script = [FakeResponse([{"id": 1}]) for _ in range(3)]
    with pytest.raises(ExtractionError, match="max_pages"):
        api_extract(monkeypatch, script, max_pages=2, pagination={"type": "page", "page_size": 1})


def test_api_auth_token_from_env(monkeypatch):
    monkeypatch.setenv("MY_TOKEN", "s3cret")
    res, s = api_extract(monkeypatch, [FakeResponse([{"id": 1}])], auth={"type": "bearer", "token_env": "MY_TOKEN"})
    assert s.calls[0]["headers"]["Authorization"] == "Bearer s3cret"
    monkeypatch.delenv("MY_TOKEN")
    with pytest.raises(ExtractionError, match="MY_TOKEN"):
        api_extract(monkeypatch, [FakeResponse([])], auth={"type": "bearer", "token_env": "MY_TOKEN"})


# ------------------------------------------------------------------------------ SQL
@pytest.fixture
def source_db(tmp_path, monkeypatch):
    path = tmp_path / "src.db"
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE items (id INTEGER, name TEXT, active INTEGER)")
    con.executemany("INSERT INTO items VALUES (?,?,?)", [(1, "a", 1), (2, "b", 0), (3, "c", 1)])
    con.commit()
    con.close()
    monkeypatch.setenv("SRC_URL", f"sqlite:///{path}")


def sql_cfg(query, **kw):
    return {"type": "sql", "connection_env": "SRC_URL", "query": query, **kw}


def test_sql_select_with_bound_params(source_db):
    res = extract(sql_cfg("SELECT id, name FROM items WHERE active = :a", params={"a": 1}))
    assert res.data["id"].tolist() == [1, 3]
    assert res.metadata["row_count"] == 2


def test_sql_empty_result(source_db):
    assert extract(sql_cfg("SELECT * FROM items WHERE id > 100")).data.empty


def test_sql_rejects_non_select_and_missing_env(source_db, monkeypatch):
    with pytest.raises(ExtractionError, match="SELECT"):
        extract(sql_cfg("DELETE FROM items"))
    monkeypatch.delenv("SRC_URL")
    with pytest.raises(ExtractionError, match="SRC_URL"):
        extract(sql_cfg("SELECT 1"))


def test_sql_failure_becomes_extraction_error(source_db):
    with pytest.raises(ExtractionError, match="query failed"):
        extract(sql_cfg("SELECT * FROM no_such_table"))
