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
    print("Setting up db...")
    fp = pathlib.Path(__file__).parent / "fixtures" / "setup_db.sql"
    run_queries_in_file(con=con, fp=fp)
    con.execute("ANALYZE hr.employee;")
    con.execute("ANALYZE sales.customer;")
    con.execute("ANALYZE sales.employee_customer;")


def tear_down_db(con: pyodbc.Connection, /) -> None:
    print("Tearing down db...")
    fp = pathlib.Path(__file__).parent / "fixtures" / "tear_down_db.sql"
    run_queries_in_file(con=con, fp=fp)


def clear_cache(cache_dir: pathlib.Path, /) -> None:
    if cache_dir.exists():
        for fp in cache_dir.iterdir():
            if fp.suffix == ".p":
                fp.unlink()


@pytest.fixture(scope="function")
def pg_cursor(cache_dir: pathlib.Path) -> typing.Generator[pyodbc.Cursor, None, None]:
    clear_cache(cache_dir)
    db_uri = os.environ["PYODBC_URI"]
    with pyodbc.connect(db_uri) as con:
        set_up_db(con)
        with con.cursor() as cur:
            yield cur
        tear_down_db(con)
