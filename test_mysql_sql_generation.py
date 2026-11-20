"""No MySQL server is needed: these tests only check the SQL we would send to MySQL."""
from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine
from sqlalchemy.dialects import mysql

from src.db.models import metadata
from src.loaders.mysql_loader import MySQLLoader

RETRY = {"max_attempts": 1, "backoff_seconds": 0, "backoff_factor": 1}
T = Table("people", MetaData(), Column("id", Integer, primary_key=True), Column("name", String(50)))


def compiled(mode, on_duplicate):
    engine = create_engine("mysql+pymysql://u:p@localhost/db")      # lazy: never connects
    loader = MySQLLoader(engine, "people", mode, 10, ["id"], on_duplicate, RETRY)
    stmt = loader._insert_stmt(T, ["id", "name"])
    return str(stmt.compile(dialect=mysql.dialect()))


def test_upsert_uses_on_duplicate_key_update():
    sql = compiled("upsert", "update")
    assert "ON DUPLICATE KEY UPDATE name = VALUES(name)" in sql or "AS new" in sql


def test_append_skip_is_a_noop_update_never_insert_ignore():
    sql = compiled("append", "skip")
    assert "IGNORE" not in sql and "ON DUPLICATE KEY UPDATE id = people.id" in sql


def test_append_error_is_plain_insert():
    assert "ON DUPLICATE" not in compiled("append", "error")


def test_metadata_ddl_compiles_for_mysql():
    from sqlalchemy.schema import CreateTable
    ddl = "\n".join(str(CreateTable(t).compile(dialect=mysql.dialect())) for t in metadata.sorted_tables)
    assert "LONGTEXT" in ddl and "etl_pipeline_runs" in ddl and "BIGINT" in ddl
