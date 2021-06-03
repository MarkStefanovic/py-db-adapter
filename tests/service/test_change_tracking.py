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
        ,   ch.valid_to DESC
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
    src_db_adapter = adapter.PostgresAdapter()
    dest_db_adapter = adapter.PostgresAdapter()

    service.update_history_table(
        src_cur=pg_cursor,
        dest_cur=pg_cursor,
        src_db_adapter=src_db_adapter,
        dest_db_adapter=dest_db_adapter,
        src_schema_name="sales",
        src_table_name="customer",
        compare_cols=None,  # compare all columns
        recreate=False,
    )
    check_history_table_in_sync(cur=pg_cursor)


def test_change_tracking_after_update(pg_cursor: pyodbc.Cursor) -> None:
    src_db_adapter = adapter.PostgresAdapter()
    dest_db_adapter = adapter.PostgresAdapter()

    pg_cursor.execute(
        """
            UPDATE sales.customer 
            SET customer_last_name = 'Smithers' 
            WHERE customer_first_name = 'Steve'
        """
    )
    pg_cursor.commit()
    result = pg_cursor.execute(
        """
            SELECT c.customer_last_name 
            FROM sales.customer AS c
            WHERE c.customer_first_name = 'Steve'
        """
    ).fetchval()
    assert result == "Smithers"

    service.update_history_table(
        src_cur=pg_cursor,
        dest_cur=pg_cursor,
        src_db_adapter=src_db_adapter,
        dest_db_adapter=dest_db_adapter,
        src_schema_name="sales",
        src_table_name="customer",
        compare_cols=None,  # compare all columns
        recreate=False,
    )
    check_history_table_in_sync(cur=pg_cursor)

    pg_cursor.execute(
        """
            UPDATE sales.customer 
            SET customer_last_name = 'Smalls' 
            WHERE customer_first_name = 'Steve'
        """
    )
    pg_cursor.commit()
    service.update_history_table(
        src_cur=pg_cursor,
        dest_cur=pg_cursor,
        src_db_adapter=src_db_adapter,
        dest_db_adapter=dest_db_adapter,
        src_schema_name="sales",
        src_table_name="customer",
        compare_cols=None,  # compare all columns
        recreate=False,
    )
    check_history_table_in_sync(cur=pg_cursor)
    result = pg_cursor.execute(
        """
            SELECT h.customer_last_name 
            FROM sales.customer_history AS h
            WHERE 
                h.customer_first_name = 'Steve'
                AND h.valid_to = '9999-12-31'
        """
    ).fetchall()
    assert result is not None
    assert len(result) == 1
    assert result[0].customer_last_name == "Smalls"


def test_change_tracking_after_delete(pg_cursor: pyodbc.Cursor) -> None:
    src_db_adapter = adapter.PostgresAdapter()
    dest_db_adapter = adapter.PostgresAdapter()

    pg_cursor.execute("DELETE FROM sales.customer WHERE customer_first_name = 'Dan'")
    result = pg_cursor.execute(
        """
            SELECT 1 
            FROM sales.customer AS c
            WHERE c.customer_first_name = 'Dan'
        """
    ).fetchone()
    assert result is None

    service.update_history_table(
        src_cur=pg_cursor,
        dest_cur=pg_cursor,
        src_db_adapter=src_db_adapter,
        dest_db_adapter=dest_db_adapter,
        src_schema_name="sales",
        src_table_name="customer",
        compare_cols=None,  # compare all columns
        recreate=False,
    )
    result = pg_cursor.execute(
        """
        SELECT 1 
        FROM sales.customer_history AS h
        WHERE 
            h.customer_first_name = 'Dan' 
            AND h.valid_to = '9999-12-31'
    """
    ).fetchone()
    assert result is None

    pg_cursor.execute(
        """
        INSERT INTO sales.customer (
            customer_first_name
        ,   customer_last_name
        ,   date_added
        )
        VALUES (
            'Zardon'
        ,   'Zarbos'
        ,   CAST('2010-01-02 03:04:05' AS TIMESTAMP)
        )
    """
    )
    result = pg_cursor.execute(
        """
        SELECT COUNT(*) 
        FROM sales.customer 
        WHERE customer_first_name = 'Zardon'
    """
    ).fetchval()
    assert result == 1
    service.update_history_table(
        src_cur=pg_cursor,
        dest_cur=pg_cursor,
        src_db_adapter=src_db_adapter,
        dest_db_adapter=dest_db_adapter,
        src_schema_name="sales",
        src_table_name="customer",
        compare_cols=None,  # compare all columns
        recreate=False,
    )
    result = pg_cursor.execute(
        """
        SELECT COUNT(*) 
        FROM sales.customer_history AS h
        WHERE 
            h.customer_first_name = 'Zardon'
            AND h.valid_to = '9999-12-31'
    """
    ).fetchval()
    assert result == 1
