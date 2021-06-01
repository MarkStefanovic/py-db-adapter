import pyodbc

from py_db_adapter import adapter, domain, service


def test_compare_rows_using_defaults(pg_cursor: pyodbc.Cursor) -> None:
    src_db_adapter = adapter.PostgresAdapter()
    dest_db_adapter = adapter.PostgresAdapter()
    result = service.compare_rows(
        src_cur=pg_cursor,
        dest_cur=pg_cursor,
        src_db_adapter=src_db_adapter,
        dest_db_adapter=dest_db_adapter,
        src_schema_name="sales",
        src_table_name="customer",
        dest_schema_name="sales",
        dest_table_name="customer2",
    )
    assert isinstance(result, domain.RowComparisonResult)
    assert result == "test"
