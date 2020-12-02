from py_db_adapter import adapter


def test_postgres_pyodbc_db_adapter_table_exists(postgres_pyodbc_db_uri: str) -> None:
    sql_adapter = adapter.PostgreSQLAdapter()
    con = adapter.PyodbcConnection(
        db_name="test_con",
        fast_executemany=False,
        uri=postgres_pyodbc_db_uri,
        autocommit=False,
    )
    db_adapter = adapter.PostgresPyodbcDbAdapter(
        con=con, postgres_sql_adapter=sql_adapter
    )
    with con:
        assert not db_adapter.table_exists(
            schema_name="sales", table_name="this_does_not_exist"
        )
        assert db_adapter.table_exists(schema_name="sales", table_name="customer")


def test_postgres_pyodbc_db_adapter_fast_row_count(postgres_pyodbc_db_uri: str) -> None:
    sql_adapter = adapter.PostgreSQLAdapter()
    con = adapter.PyodbcConnection(
        db_name="test_con",
        fast_executemany=False,
        uri=postgres_pyodbc_db_uri,
        autocommit=False,
    )
    db_adapter = adapter.PostgresPyodbcDbAdapter(
        con=con, postgres_sql_adapter=sql_adapter
    )
    with con:
        rows = db_adapter.fast_row_count(schema_name="sales", table_name="customer")
    assert rows == 9
