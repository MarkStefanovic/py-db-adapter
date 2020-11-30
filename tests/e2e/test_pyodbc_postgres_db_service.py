import datetime
import pathlib

import pyodbc

import py_db_adapter as pda


def check_customer2_table_in_sync(db_uri: str) -> None:
    with pyodbc.connect(db_uri) as con:
        with con.cursor() as cur:
            sql = "SELECT * FROM sales.customer2"
            result = cur.execute(sql).fetchall()
            actual = {
                tuple(zip((col[0] for col in cur.description), row)) for row in result
            }
    expected = {
        (
            ("customer_id", 1),
            ("customer_first_name", "Amy"),
            ("customer_last_name", "Adamant"),
            ("date_added", datetime.datetime(2010, 1, 2, 3, 4, 5)),
            ("date_updated", None),
        ),
        (
            ("customer_id", 2),
            ("customer_first_name", "Billy"),
            ("customer_last_name", "Bob"),
            ("date_added", datetime.datetime(2010, 2, 3, 4, 5, 6)),
            ("date_updated", None),
        ),
        (
            ("customer_id", 3),
            ("customer_first_name", "Chris"),
            ("customer_last_name", "Claus"),
            ("date_added", datetime.datetime(2010, 4, 5, 6, 7, 8)),
            ("date_updated", None),
        ),
        (
            ("customer_id", 4),
            ("customer_first_name", "Dan"),
            ("customer_last_name", "Danger"),
            ("date_added", datetime.datetime(2010, 9, 10, 11, 12, 13)),
            ("date_updated", None),
        ),
        (
            ("customer_id", 5),
            ("customer_first_name", "Eric"),
            ("customer_last_name", "Eerie"),
            ("date_added", datetime.datetime(2010, 4, 15, 6, 17, 18)),
            ("date_updated", None),
        ),
        (
            ("customer_id", 6),
            ("customer_first_name", "Fred"),
            ("customer_last_name", "Finkle"),
            ("date_added", datetime.datetime(2010, 9, 20, 1, 22, 23)),
            ("date_updated", None),
        ),
        (
            ("customer_id", 7),
            ("customer_first_name", "George"),
            ("customer_last_name", "Goose"),
            ("date_added", datetime.datetime(2010, 4, 25, 6, 27, 28)),
            ("date_updated", None),
        ),
        (
            ("customer_id", 8),
            ("customer_first_name", "Mandie"),
            ("customer_last_name", "Mandelbrot"),
            ("date_added", datetime.datetime(2010, 9, 30, 1, 32, 33)),
            ("date_updated", None),
        ),
        (
            ("customer_id", 9),
            ("customer_first_name", "Steve"),
            ("customer_last_name", "Smith"),
            ("date_added", datetime.datetime(2010, 4, 5, 6, 37, 38)),
            ("date_updated", None),
        ),
    }
    assert actual == expected


def test_inspect_table(cache_dir: pathlib.Path, postgres_pyodbc_db_uri: str) -> None:
    db_service = pda.PostgresPyodbcDbService(
        db_name="pg_local",
        pyodbc_uri=postgres_pyodbc_db_uri,
        cache_dir=cache_dir,
    )
    table_def = db_service._inspect_table(schema_name="sales", table_name="customer")
    assert table_def.column_names == {
        "customer_first_name",
        "customer_id",
        "customer_last_name",
        "date_added",
        "date_updated",
    }


def test_upsert_with_explicit_cols(
    cache_dir: pathlib.Path, postgres_pyodbc_db_uri: str
) -> None:
    db_service = pda.PostgresPyodbcDbService(
        db_name="pg_local",
        pyodbc_uri=postgres_pyodbc_db_uri,
        cache_dir=cache_dir,
    )
    db_service.upsert_table(
        src_db=db_service,
        src_schema_name="sales",
        src_table_name="customer",
        dest_schema_name="sales",
        dest_table_name="customer2",
        pk_cols={"customer_id"},
        compare_cols={"customer_first_name", "customer_last_name"},
        add=True,
        update=True,
        delete=True,
    )
    check_customer2_table_in_sync(postgres_pyodbc_db_uri)


def test_upsert_with_default_cols(cache_dir: pathlib.Path, postgres_pyodbc_db_uri: str) -> None:
    db_service = pda.PostgresPyodbcDbService(
        db_name="pg_local",
        pyodbc_uri=postgres_pyodbc_db_uri,
        cache_dir=cache_dir,
    )

    with pyodbc.connect(postgres_pyodbc_db_uri) as con:
        with con.cursor() as cur:
            row_ct_sql = "SELECT COUNT(*) AS row_ct FROM sales.customer2"
            row_ct = cur.execute(row_ct_sql).fetchval()
            assert row_ct == 0

    db_service.upsert_table(
        src_db=db_service,
        src_schema_name="sales",
        src_table_name="customer",
        dest_schema_name="sales",
        dest_table_name="customer2",
        pk_cols=None,
        compare_cols=None,
        add=True,
        update=True,
        delete=True,
    )
    check_customer2_table_in_sync(postgres_pyodbc_db_uri)


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
