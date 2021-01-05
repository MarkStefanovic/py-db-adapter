import os
import pathlib
import typing

import dotenv
import pyodbc
import pytest

from py_db_adapter import domain

dotenv.load_dotenv(dotenv.find_dotenv())


@pytest.fixture(scope="session")
def cache_dir() -> pathlib.Path:
    return pathlib.Path(os.environ["CACHE_DIR"])


def read_sql(fp: pathlib.Path, /) -> typing.List[str]:
    with fp.open(mode="r") as fh:
        return [
            sql
            for stmt in fh.read().split(";")
            if (sql := domain.standardize_sql(stmt))
        ]


def run_queries_in_file(*, con: pyodbc.Connection, fp: pathlib.Path) -> None:
    with con.cursor() as cur:
        for sql in read_sql(fp):
            cur.execute(sql)


def set_up_db(con: pyodbc.Connection, /) -> None:
    fp = pathlib.Path(__file__).parent / "fixtures" / "setup_db.sql"
    run_queries_in_file(con=con, fp=fp)


def tear_down_db(con: pyodbc.Connection, /) -> None:
    fp = pathlib.Path(__file__).parent / "fixtures" / "tear_down_db.sql"
    run_queries_in_file(con=con, fp=fp)


# @pytest.mark.fixture(scope="session")
# def engine() -> sa.engine.Engine:
#     return sa.create_engine(os.environ["SQLALCHEMY_URI"])


def clear_cache(cache_dir: pathlib.Path, /) -> None:
    if cache_dir.exists():
        for fp in cache_dir.iterdir():
            if fp.suffix == ".p":
                fp.unlink()


@pytest.fixture(scope="function")
def postgres_pyodbc_db_uri(cache_dir: pathlib.Path) -> typing.Generator[str, None, None]:
    clear_cache(cache_dir)

    db_uri = os.environ["PYODBC_URI"]
    with pyodbc.connect(db_uri) as con:
        set_up_db(con)
        yield db_uri
        tear_down_db(con)


# @pytest.fixture(scope="session")
# def pyodbc_postgres_con(postgres_pyodbc_db_uri: str) -> pyodbc.Connection:
#     with pyodbc.connect(postgres_pyodbc_db_uri) as con:
#         setup_db(con)
#         yield con


# @pytest.fixture(scope="session")
# def employee_sql_table_adapter(
#     pyodbc_postgres_con: pyodbc.Connection,
# ) -> adapter.PostgreSQLAdapter:
#     table = adapter.pyodbc_inspect_table(
#         con=pyodbc_postgres_con,
#         table_name="employee",
#         schema_name="hr",
#     )
#     return adapter.PostgreSQLAdapter()
#
#
# @pytest.fixture(scope="session")
# def customer_sql_table_adapter(
#     pyodbc_postgres_con: pyodbc.Connection,
# ) -> adapter.PostgreSQLAdapter:
#     table = adapter.pyodbc_inspect_table(
#         con=pyodbc_postgres_con,
#         table_name="customer",
#         schema_name="sales",
#     )
#     return adapter.PostgreSQLAdapter()

# if __name__ == "__main__":
#     fp = pathlib.Path("./fixtures/hr.employee.sql")
#     with fp.open(mode="r") as fh:
#         sql_statments = [
#             sql
#             for stmt in fh.read().split(";")
#             if (sql := adapter.standardize_sql(stmt))
#         ]
#         print(f"{sql_statments=}")
#
#     con_str = os.environ["PYODBC_URI"]
#     with pyodbc.connect(con_str) as con:
#         for qry in sql_statments:
#             con.execute(qry)
