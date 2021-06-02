import decimal

import pyodbc

from py_db_adapter import adapter, domain, service


def test_compare_rows_when_dest_is_empty(pg_cursor: pyodbc.Cursor) -> None:
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
        missing_row_examples="(customer_id): (1), (2), (3), (4), (5), (6), (7), (8), (9)",
        pct_missing=decimal.Decimal("1"),
        extra_rows=0,
        extra_row_examples="",
        pct_extra=decimal.Decimal("0"),
        stale_rows=0,
        stale_row_examples="",
        pct_stale=decimal.Decimal("0"),
    )
    assert result == expected

    insert_sql = """
        INSERT INTO sales.customer (
            customer_id
        ,   customer_first_name
        ,   customer_last_name
        ,   date_added
        )
        VALUES
            (1, 'Amy', 'Adamant', CAST('2010-01-02 03:04:05' AS TIMESTAMP))
        ,   (2, 'Billy', 'Bob', CAST('2010-02-03 04:05:06' AS TIMESTAMP))
        ,   (3, 'Chris', 'Claus', CAST('2010-04-05 06:07:08' AS TIMESTAMP))
        ,   (4, 'Dan', 'Danger', CAST('2010-09-10 11:12:13' AS TIMESTAMP))
        ,   (5, 'Eric', 'Eerie', CAST('2010-04-15 06:17:18' AS TIMESTAMP))
        ,   (6, 'Fred', 'Finkle', CAST('2010-09-20 01:22:23' AS TIMESTAMP))
        ,   (7, 'George', 'Goose', CAST('2010-04-25 06:27:28' AS TIMESTAMP))
        ,   (8, 'Mandie', 'Mandelbrot', CAST('2010-09-30 01:32:33' AS TIMESTAMP))
        ,   (9, 'Steve', 'Smith', CAST('2010-04-05 06:37:38' AS TIMESTAMP))
        ;
    """


def test_compare_rows_when_sources_match(pg_cursor: pyodbc.Cursor) -> None:
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
        dest_table_name="customer",
    )
    assert isinstance(result, domain.RowComparisonResult)
    expected = domain.RowComparisonResult(
        src_schema="sales",
        src_table="customer",
        dest_schema="sales",
        dest_table="customer",
        missing_rows=0,
        missing_row_examples="",
        pct_missing=decimal.Decimal("0"),
        extra_rows=0,
        extra_row_examples="",
        pct_extra=decimal.Decimal("0"),
        stale_rows=0,
        stale_row_examples="",
        pct_stale=decimal.Decimal("0"),
    )
    assert result == expected
