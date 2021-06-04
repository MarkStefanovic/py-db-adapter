import datetime
import decimal

import freezegun
import pyodbc

from py_db_adapter import adapter, domain, service


def test_compare_rows_when_dest_is_empty(pg_cursor: pyodbc.Cursor) -> None:
    src_db_adapter = adapter.PostgresAdapter()
    dest_db_adapter = adapter.PostgresAdapter()
    with freezegun.freeze_time("2010-01-01"):
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
        src_rows=9,
        dest_rows=0,
        missing_rows=9,
        missing_row_examples="(customer_id): (1), (2), (3), (4), (5), (6), (7), (8), (9)",
        pct_missing=decimal.Decimal("1"),
        extra_rows=0,
        extra_row_examples="",
        pct_extra=decimal.Decimal("0"),
        stale_rows=0,
        stale_row_examples="",
        pct_stale=decimal.Decimal("0"),
        ts=datetime.datetime(2010, 1, 1),
        error_message=None,
        traceback=None,
    )
    assert result == expected


def test_compare_rows_when_sources_match(pg_cursor: pyodbc.Cursor) -> None:
    src_db_adapter = adapter.PostgresAdapter()
    dest_db_adapter = adapter.PostgresAdapter()
    with freezegun.freeze_time("2010-01-01"):
        result = service.compare_rows(
            src_cur=pg_cursor,
            dest_cur=pg_cursor,
            src_db_adapter=src_db_adapter,
            dest_db_adapter=dest_db_adapter,
            src_schema_name="sales",
            src_table_name="customer",
            dest_schema_name="sales",
            dest_table_name="customer",
        )
    assert isinstance(result, domain.RowComparisonResult)
    expected = domain.RowComparisonResult(
        src_schema="sales",
        src_table="customer",
        dest_schema="sales",
        dest_table="customer",
        src_rows=9,
        dest_rows=9,
        missing_rows=0,
        missing_row_examples="",
        pct_missing=decimal.Decimal("0"),
        extra_rows=0,
        extra_row_examples="",
        pct_extra=decimal.Decimal("0"),
        stale_rows=0,
        stale_row_examples="",
        pct_stale=decimal.Decimal("0"),
        ts=datetime.datetime(2010, 1, 1),
        error_message=None,
        traceback=None,
    )
    assert result == expected
