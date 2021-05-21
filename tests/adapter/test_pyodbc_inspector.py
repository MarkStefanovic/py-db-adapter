import pyodbc

from py_db_adapter import adapter


def test_inspect_table(pg_cursor: pyodbc.Cursor) -> None:
    tbl = adapter.inspect_table(
        cur=pg_cursor,
        cache_dir=None,
        schema_name="sales",
        table_name="customer",
        pk_cols=None,
        include_cols=None,
    )
    assert tbl.column_names == {
        "customer_first_name",
        "customer_id",
        "customer_last_name",
        "date_added",
        "date_updated",
    }
