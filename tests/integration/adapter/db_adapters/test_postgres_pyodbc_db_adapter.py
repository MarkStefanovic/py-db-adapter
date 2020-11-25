from py_db_adapter.adapter.db_adapters import postgres_pyodbc_db_adapter
from py_db_adapter.adapter.db_connections import pyodbc_connection
from py_db_adapter.adapter.sql_adapters import postgres_sql_adapter


def test_postgres_pyodbc_db_adapter_table_exists(postgres_pyodbc_db_uri: str) -> None:
    sql_adapter = postgres_sql_adapter.PostgreSQLAdapter()
    con = pyodbc_connection.PyodbcConnection(
        db_name="test_con", fast_executemany=False, uri=postgres_pyodbc_db_uri
    )
    db_adapter = postgres_pyodbc_db_adapter.PostgresPyodbcDbAdapter(
        con=con, postgres_sql_adapter=sql_adapter
    )
    with con:
        assert not db_adapter.table_exists(
            schema_name="sales", table_name="this_does_not_exist"
        )
        assert db_adapter.table_exists(schema_name="sales", table_name="customer")


def test_postgres_pyodbc_db_adapter_fast_row_count(postgres_pyodbc_db_uri: str) -> None:
    sql_adapter = postgres_sql_adapter.PostgreSQLAdapter()
    con = pyodbc_connection.PyodbcConnection(
        db_name="test_con", fast_executemany=False, uri=postgres_pyodbc_db_uri
    )
    db_adapter = postgres_pyodbc_db_adapter.PostgresPyodbcDbAdapter(
        con=con, postgres_sql_adapter=sql_adapter
    )
    with con:
        rows = db_adapter.fast_row_count(schema_name="sales", table_name="customer")
    assert rows == 9
