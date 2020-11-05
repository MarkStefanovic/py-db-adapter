import datetime

import pyodbc

from py_db_adapter import domain, adapter, service


def test_insert_with_manual_pk_value(
    pyodbc_postgres_con: pyodbc.Connection,
    customer_sql_table_adapter: adapter.SqlTableAdapter,
):
    rows = [
        {
            "customer_id": 100,
            "customer_first_name": "Mark",
            "customer_last_name": "Stefanovic",
            "date_added": datetime.datetime(2010, 1, 2, 3, 4, 5),
            "date_updated": None,
        }
    ]
    service.insert_rows(
        sql_adapter=customer_sql_table_adapter, con=pyodbc_postgres_con, rows=rows
    )
    service.delete_rows(
        sql_adapter=customer_sql_table_adapter, con=pyodbc_postgres_con, pk_values=[{"customer_id": (100,)}]
    )


def test_insert_with_manual_pk_value(
    pyodbc_postgres_con: pyodbc.Connection,
    customer_sql_table_adapter: adapter.SqlTableAdapter,
):
    rows = [
        {
            "customer_id": 100,
            "customer_first_name": "Mark",
            "customer_last_name": "Stefanovic",
            "date_added": datetime.datetime(2010, 1, 2, 3, 4, 5),
            "date_updated": None,
        }
    ]
    service.insert_rows(
        sql_adapter=customer_sql_table_adapter, con=pyodbc_postgres_con, rows=rows
    )
