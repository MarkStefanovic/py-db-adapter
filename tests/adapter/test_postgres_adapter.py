import time

import pyodbc

from py_db_adapter import adapter


def test_postgres_adapter_when_table_exists(pg_cursor: pyodbc.Cursor) -> None:
    db_adapter = adapter.PostgresAdapter()
    assert db_adapter.table_exists(
        cur=pg_cursor, schema_name="sales", table_name="customer"
    )


def test_postgres_adapter_when_table_does_not_exists(
    pg_cursor: pyodbc.Cursor,
) -> None:
    db_adapter = adapter.PostgresAdapter()
    assert not db_adapter.table_exists(
        cur=pg_cursor, schema_name="sales", table_name="this_does_not_exist"
    )


def test_postgres_adapter_fast_row_count(pg_cursor: pyodbc.Cursor) -> None:
    stats_sql = """
        SELECT
            pc.reltuples
        ,   tbl.n_live_tup
        ,   tbl.n_dead_tup
        ,   tbl.n_mod_since_analyze
        ,   tbl.last_analyze
        ,   tbl.last_autoanalyze
        ,   tbl.last_vacuum
        ,   tbl.last_autovacuum
        ,   CASE WHEN tbl.last_analyze IS NULL AND tbl.last_autoanalyze IS NULL THEN 0 ELSE 1 END AS has_stats
        FROM pg_catalog.pg_class AS pc
        JOIN pg_stat_all_tables tbl
            ON pc.oid = tbl.relid
        WHERE
            pc.relname = 'customer'
            AND tbl.schemaname = 'sales'
    """
    result = pg_cursor.execute(stats_sql).fetchall()
    for row in result:
        print(row)
    db_adapter = adapter.PostgresAdapter()
    rows = db_adapter.fast_row_count(
        cur=pg_cursor, schema_name="sales", table_name="customer"
    )
    assert rows == 9
