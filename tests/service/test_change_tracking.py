import pyodbc

from py_db_adapter import adapter, service


def check_history_table_in_sync(cur: pyodbc.Cursor) -> None:
    sql = "SELECT COUNT(*) FROM sales.customer"
    result = cur.execute(sql).fetchval()
    assert result > 0

    sql = """
        SELECT 
            c.customer_id
        ,   c.customer_first_name
        ,   c.customer_last_name
        FROM sales.customer AS c
    """
    result = cur.execute(sql).fetchall()
    customer_values = {
        tuple(zip((col[0] for col in cur.description), row)) for row in result
    }
    sql = """
        SELECT DISTINCT ON (ch.customer_id) 
            ch.customer_id
        ,   ch.customer_first_name
        ,   ch.customer_last_name
        FROM sales.customer_history AS ch
        ORDER BY 
            ch.customer_id
        ,   ch.date_updated DESC NULLS LAST
        ,   ch.date_added DESC
    """
    result = cur.execute(sql).fetchall()
    customer2_values = {
        tuple(zip((col[0] for col in cur.description), row)) for row in result
    }
    assert len(customer_values) > 0
    assert len(customer2_values) == len(customer_values)
    assert (
        customer2_values == customer_values
    ), f"\ncustomer:\n{sorted(customer_values)}\n\ncustomer2:\n{sorted(customer2_values)}"


def test_change_tracking_happy_path(pg_cursor: pyodbc.Cursor) -> None:
    sql = "SELECT COUNT(*) FROM sales.customer"
    original_rows = pg_cursor.execute(sql).fetchval()
    assert original_rows > 0

    src_db_adapter = adapter.PostgresAdapter()
    dest_db_adapter = adapter.PostgresAdapter()
    src_table = adapter.inspect_table(
        cur=pg_cursor,
        schema_name="sales",
        table_name="customer",
        pk_cols=None,
        include_cols=None,
        cache_dir=None,
    )
    service.update_history_table(
        src_cur=pg_cursor,
        dest_cur=pg_cursor,
        src_db_adapter=src_db_adapter,
        dest_db_adapter=dest_db_adapter,
        src_table=src_table,
        compare_cols=None,  # compare all columns
        recreate=False,
    )
    check_history_table_in_sync(cur=pg_cursor)

    # test after an update
    sql = "UPDATE sales.customer SET customer_last_name = 'Smithers' WHERE customer_first_name = 'Steve'"
    pg_cursor.execute(sql)
    pg_cursor.commit()
    # TODO assert the sales.customer name was actually changed
    service.update_history_table(
        src_cur=pg_cursor,
        dest_cur=pg_cursor,
        src_db_adapter=src_db_adapter,
        dest_db_adapter=dest_db_adapter,
        src_table=src_table,
        compare_cols=None,  # compare all columns
        recreate=False,
    )
    check_history_table_in_sync(cur=pg_cursor)


#     with ds:
#         ds.update_history_table(recreate=False)
#         ds.commit()
#
#     with pyodbc.connect(postgres_pyodbc_db_uri) as con:
#         with con.cursor() as cur:
#             sql = "SELECT * FROM sales.customer_history WHERE customer_first_name = 'Steve'"
#             result = cur.execute(sql).fetchall()
#             updated_rows = [
#                 dict(zip((col[0] for col in cur.description), row)) for row in result
#             ]
#     assert len(updated_rows) == 2, "failed after 1st update"
#     assert sum(row["valid_to"] == datetime.datetime(9999, 12, 31) for row in updated_rows) == 1
#     original_row = next(row for row in updated_rows if row["valid_to"] != datetime.datetime(9999, 12, 31))
#     new_row = next(row for row in updated_rows if row["valid_to"] == datetime.datetime(9999, 12, 31))
#     assert original_row["valid_to"] + datetime.timedelta(microseconds=1) == new_row["valid_from"]
#
#     # 2nd update
#     with pyodbc.connect(postgres_pyodbc_db_uri) as con:
#         with con.cursor() as cur:
#             sql = "UPDATE sales.customer SET customer_last_name = 'Smalls' WHERE customer_first_name = 'Steve'"
#             cur.execute(sql)
#             cur.commit()
#
#     with ds:
#         ds.update_history_table(recreate=False)
#         ds.commit()
#
#     with pyodbc.connect(postgres_pyodbc_db_uri) as con:
#         with con.cursor() as cur:
#             sql = "SELECT * FROM sales.customer_history WHERE customer_first_name = 'Steve'"
#             result = cur.execute(sql).fetchall()
#             updated_rows = [
#                 dict(zip((col[0] for col in cur.description), row)) for row in result
#             ]
#     assert len(updated_rows) == 3
#     assert sum(row["valid_to"] == datetime.datetime(9999, 12, 31) for row in updated_rows) == 1
#     new_row = next(row for row in updated_rows if row["valid_to"] == datetime.datetime(9999, 12, 31))
#     assert new_row["customer_last_name"] == "Smalls"
#     # fmt: on
#
#
# def test_update_history_after_source_rows_deleted(
#     cache_dir: pathlib.Path, postgres_pyodbc_db_uri: str
# ) -> None:
#     ds = pda.postgres_pyodbc_datasource(
#         db_name="test_db",
#         db_uri=postgres_pyodbc_db_uri,
#         cache_dir=cache_dir,
#         schema_name="sales",
#         table_name="customer",
#         compare_cols=None,
#         custom_pk_cols=None,
#         max_batch_size=1000,
#         read_only=False,
#     )
#     with ds:
#         ds.update_history_table(recreate=False)
#         ds.commit()
#
#     with pyodbc.connect(postgres_pyodbc_db_uri) as con:
#         with con.cursor() as cur:
#             sql = "DELETE FROM sales.customer WHERE customer_first_name = 'Dan'"
#             cur.execute(sql)
#             cur.commit()
#
#     with ds:
#         ds.update_history_table(recreate=False)
#         ds.commit()
#
#     # fmt: off
#     with pyodbc.connect(postgres_pyodbc_db_uri) as con:
#         with con.cursor() as cur:
#             sql = "SELECT * FROM sales.customer_history WHERE customer_first_name = 'Dan'"
#             result = cur.execute(sql).fetchall()
#             deleted_rows = [
#                 dict(zip((col[0] for col in cur.description), row)) for row in result
#             ]
#     assert len(deleted_rows) == 1
#     assert sum(row["valid_to"] == datetime.datetime(9999, 12, 31) for row in deleted_rows) == 0
#     # fmt: on
