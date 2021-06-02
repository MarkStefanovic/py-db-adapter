import pyodbc

from py_db_adapter import adapter, service


def check_customer2_table_in_sync(cur: pyodbc.Cursor) -> None:
    sql = "SELECT COUNT(*) FROM sales.customer"
    result = cur.execute(sql).fetchval()
    assert result > 0

    sql = "SELECT * FROM sales.customer"
    result = cur.execute(sql).fetchall()
    customer_values = {
        tuple(zip((col[0] for col in cur.description), row)) for row in result
    }
    sql = "SELECT * FROM sales.customer2"
    result = cur.execute(sql).fetchall()
    customer2_values = {
        tuple(zip((col[0] for col in cur.description), row)) for row in result
    }
    assert (
        customer2_values == customer_values
    ), f"\ncustomer:\n{sorted(customer_values)}\n\ncustomer2:\n{sorted(customer2_values)}"


def test_sync_with_explicit_cols(pg_cursor: pyodbc.Cursor) -> None:
    db_adapter = adapter.PostgresAdapter()
    result = service.sync(
        src_cur=pg_cursor,
        dest_cur=pg_cursor,
        src_db_adapter=db_adapter,
        dest_db_adapter=db_adapter,
        src_schema_name="sales",
        src_table_name="customer",
        dest_schema_name="sales",
        dest_table_name="customer2",
        pk_cols=["customer_id"],
        compare_cols={"customer_first_name", "customer_last_name"},
    )
    check_customer2_table_in_sync(cur=pg_cursor)
    assert result.added == 9
    assert result.deleted == 0
    assert result.updated == 0

    # test update
    pg_cursor.execute(
        "UPDATE sales.customer SET customer_first_name = 'Frank' WHERE customer_first_name = 'Dan'"
    )
    pg_cursor.commit()
    result = service.sync(
        src_cur=pg_cursor,
        dest_cur=pg_cursor,
        src_db_adapter=db_adapter,
        dest_db_adapter=db_adapter,
        src_schema_name="sales",
        src_table_name="customer",
        dest_schema_name="sales",
        dest_table_name="customer2",
        pk_cols=["customer_id"],
        compare_cols={"customer_first_name", "customer_last_name"},
    )
    check_customer2_table_in_sync(cur=pg_cursor)
    assert result.added == 0
    assert result.deleted == 0
    assert result.updated == 1

    # test delete
    pg_cursor.execute("DELETE FROM sales.customer WHERE customer_first_name = 'Steve'")
    pg_cursor.commit()
    result = service.sync(
        src_cur=pg_cursor,
        dest_cur=pg_cursor,
        src_db_adapter=db_adapter,
        dest_db_adapter=db_adapter,
        src_schema_name="sales",
        src_table_name="customer",
        dest_schema_name="sales",
        dest_table_name="customer2",
        pk_cols=["customer_id"],
        compare_cols={"customer_first_name", "customer_last_name"},
    )
    check_customer2_table_in_sync(cur=pg_cursor)

    rows = pg_cursor.execute("SELECT COUNT(*) FROM sales.customer").fetchval()
    assert rows == 8
    assert result.added == 0
    assert result.deleted == 1
    assert result.updated == 0
