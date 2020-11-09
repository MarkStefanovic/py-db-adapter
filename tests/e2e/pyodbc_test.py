import datetime

import pyodbc

import py_db_adapter as pda


def test_insert_with_manual_pk_value(
    pyodbc_postgres_con: pyodbc.Connection,
    customer_sql_table_adapter: pda.SqlTableAdapter,
):
    # rows = [
    #     {
    #         "customer_id": 100,
    #         "customer_first_name": "Mark",
    #         "customer_last_name": "Stefanovic",
    #         "date_added": datetime.datetime(2010, 1, 2, 3, 4, 5),
    #         "date_updated": None,
    #     }
    # ]
    rows = pda.Rows(
        column_names=["customer_id", "customer_first_name", "customer_last_name", "date_added", "date_updated"],
        rows=(
            (100, "Mark", "Stefanovic", datetime.datetime(2010, 1, 2, 3, 4, 5), None),
        )
    )
    repo = pda.PyodbcDynamicRepository(
        connection=pyodbc_postgres_con,
        sql_adapter=customer_sql_table_adapter,
    )
    repo.add(rows)
    repo.delete(
        pda.Rows.from_dicts([{"customer_id": 100}]),
    )


# def test_insert_with_manual_pk_value(
#     pyodbc_postgres_con: pyodbc.Connection,
#     customer_sql_table_adapter: pda.SqlTableAdapter,
# ):
#     rows = [
#         {
#             "customer_id": 100,
#             "customer_first_name": "Mark",
#             "customer_last_name": "Stefanovic",
#             "date_added": datetime.datetime(2010, 1, 2, 3, 4, 5),
#             "date_updated": None,
#         }
#     ]
#     pda.in(
#         sql_adapter=customer_sql_table_adapter, con=pyodbc_postgres_con, rows=rows
#     )
