import decimal

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
    expected = domain.RowComparisonResult(
        src_schema="sales",
        src_table="customer",
        dest_schema="sales",
        dest_table="customer2",
        missing_rows=9,
        missing_row_examples="(customer_id): (6), (2), (5), (8), (4), (1), (7), (3), (9)",
        pct_missing=decimal.Decimal("1"),
        extra_rows=0,
        extra_row_examples="",
        pct_extra=decimal.Decimal("0"),
        stale_rows=0,
        stale_row_examples="",
        pct_stale=decimal.Decimal("0"),
    )
    assert result == expected
