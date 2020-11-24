import pyodbc

from py_db_adapter.adapter.sql_adapters import postgres_sql_adapter
from py_db_adapter.adapter.db_connections import pyodbc_connection
from py_db_adapter.adapter.db_adapters import postgres_pyodbc_db_adapter


def test_postgres_pyodbc_db_adapter(postgres_pyodbc_db_uri: str):
    con = pyodbc_connection.PyodbcConnection(
        db_name="test_con", fast_executemany=False, uri=postgres_pyodbc_db_uri
    )
    sql_adapter = postgres_sql_adapter.PostgreSQLAdapter()
    db_adapter = postgres_pyodbc_db_adapter.PostgresPyodbcDbAdapter(
        con=con, postgres_sql_adapter=sql_adapter
    )
    assert not db_adapter.table_exists(schema_name="sales", table_name="this_does_not_exist")
    assert db_adapter.table_exists(schema_name="sales", table_name="customer")
