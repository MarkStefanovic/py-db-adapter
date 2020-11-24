import datetime
import os

import pyodbc
import pytest

import py_db_adapter as pda


# @pytest.fixture(scope="session")
# def backup_customer_sql_table_adapter(
#     pyodbc_postgres_con: pyodbc.Connection,
# ) -> pda.PostgreSQLAdapter:
#     table = pda.pyodbc_inspect_table(
#         con=pyodbc_postgres_con,
#         table_name="customer2",
#         schema_name="sales",
#         custom_pk_cols=["customer_id"],
#     )
#     return pda.PostgreSQLAdapter(table=table)
#
#
# def test_insert_with_manual_pk_value(
#     customer_sql_table_adapter: pda.SqlAdapter,
# ) -> None:
#     with pyodbc.connect(os.environ["PYODBC_URI"]) as con:
#         rows = pda.Rows(
#             column_names=[
#                 "customer_id",
#                 "customer_first_name",
#                 "customer_last_name",
#                 "date_added",
#                 "date_updated",
#             ],
#             rows=(
#                 (
#                     100,
#                     "Mark",
#                     "Stefanovic",
#                     datetime.datetime(2010, 1, 2, 3, 4, 5),
#                     None,
#                 ),
#             ),
#         )
#         repo = py_db_adapter.adapter.repositories.pyodbc_repository.PyodbcRepository(
#             connection=con,
#             sql_adapter=customer_sql_table_adapter,
#             fast_executemany=False,
#         )
#         repo.add(rows)
#         repo.delete(
#             pda.Rows.from_dicts([{"customer_id": 100}]),
#         )
#
#
# def test_insert_with_auto_pk_value(
#     pyodbc_postgres_con: pyodbc.Connection,
#     customer_sql_table_adapter: pda.SqlAdapter,
#     backup_customer_sql_table_adapter: pda.SqlAdapter,
# ) -> None:
#     # rows = [
#     #     {
#     #         "customer_id": 100,
#     #         "customer_first_name": "Mark",
#     #         "customer_last_name": "Stefanovic",
#     #         "date_added": datetime.datetime(2010, 1, 2, 3, 4, 5),
#     #         "date_updated": None,
#     #     }
#     # ]
#     rows = pda.Rows(
#         column_names=[
#             "customer_first_name",
#             "customer_last_name",
#             "date_added",
#             "date_updated",
#         ],
#         rows=(("Mark", "Stefanovic", datetime.datetime(2010, 1, 2, 3, 4, 5), None),),
#     )
#     repo = py_db_adapter.adapter.repositories.pyodbc_repository.PyodbcRepository(
#         connection=pyodbc_postgres_con,
#         sql_adapter=customer_sql_table_adapter,
#         fast_executemany=False,  # fast_executemany doesn't work on PostgreSQL
#     )
#     repo.add(rows)
#     repo.update(
#         pda.Rows.from_dicts(
#             [
#                 {
#                     "customer_id": 1,
#                     "customer_first_name": "Jon",
#                     "customer_last_name": "Jinglehammer Smith",
#                 }
#             ]
#         )
#     )
#     pyodbc_postgres_con.commit()
#     print("BEFORE DELETE")
#     with pyodbc_postgres_con.cursor() as cur:
#         result = cur.execute("SELECT * FROM sales.customer").fetchall()
#         for row in result:
#             print(row)
#
#     repo.delete(
#         pda.Rows.from_dicts([{"customer_id": 4}]),
#     )
#     print("AFTER DELETE")
#     with pyodbc_postgres_con.cursor() as cur:
#         result = cur.execute("SELECT * FROM sales.customer").fetchall()
#         for row in result:
#             print(row)
#
#     pks_to_fetch = [
#         {"customer_id": 1},
#         {"customer_id": 3},
#         {"customer_id": 4},
#     ]
#     pk_rows = pda.Rows.from_dicts(pks_to_fetch)  # type: ignore
#     pks = repo.fetch_rows_by_primary_key_values(rows=pk_rows, columns=None)
#     print(f"{pks=!s}")
#
#     backup_repo = py_db_adapter.adapter.repositories.pyodbc_repository.PyodbcRepository(
#         connection=pyodbc_postgres_con,
#         sql_adapter=backup_customer_sql_table_adapter,
#         fast_executemany=False,  # fast_executemany doesn't work on PostgreSQL
#     )
#     print("sales.customer2 before upsert")
#     with pyodbc_postgres_con.cursor() as cur:
#         result = cur.execute("SELECT * FROM sales.customer2").fetchall()
#         for row in result:
#             print(row)
#     backup_repo.upsert_table(source_repo=repo)
#     pyodbc_postgres_con.commit()
#     print("sales.customer2 after upsert")
#     with pyodbc_postgres_con.cursor() as cur:
#         result = cur.execute("SELECT * FROM sales.customer2").fetchall()
#         for row in result:
#             print(row)

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
