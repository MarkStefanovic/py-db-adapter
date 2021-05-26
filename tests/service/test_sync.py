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
    service.sync(
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

    # test update
    pg_cursor.execute(
        "UPDATE sales.customer SET customer_first_name = 'Frank' WHERE customer_first_name = 'Dan'"
    )
    pg_cursor.commit()
    service.sync(
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

    # test delete
    pg_cursor.execute("DELETE FROM sales.customer WHERE customer_first_name = 'Steve'")
    pg_cursor.commit()
    service.sync(
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


# def test_sync_with_default_cols(
#     cache_dir: pathlib.Path, postgres_pyodbc_db_uri: str
# ) -> None:
#     with pyodbc.connect(postgres_pyodbc_db_uri) as con:
#         with con.cursor() as cur:
#             row_ct_sql = "SELECT COUNT(*) AS row_ct FROM sales.customer2"
#             row_ct = cur.execute(row_ct_sql).fetchval()
#             assert row_ct == 0
#
#     src = pda.postgres_pyodbc_datasource(
#         db_name="test_db",
#         db_uri=postgres_pyodbc_db_uri,
#         cache_dir=cache_dir,
#         schema_name="sales",
#         table_name="customer",
#         compare_cols=None,
#         custom_pk_cols=None,
#         max_batch_size=1000,
#         read_only=True,
#     )
#     dest = src.copy(
#         update={
#             "table_name": "customer2",
#             "read_only": False,
#         }
#     )
#     with src, dest:
#         dest.sync(src=src, recreate=False)
#         dest.commit()
#         check_customer2_table_in_sync(postgres_pyodbc_db_uri)
#
#
