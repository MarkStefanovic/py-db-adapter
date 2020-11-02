import os

import dotenv
import pyodbc
import pytest

import sqlalchemy as sa

from py_db_adapter import adapter

dotenv.load_dotenv(dotenv.find_dotenv())


@pytest.mark.fixture(scope="session")
def engine():
    return sa.create_engine(os.environ["SQLALCHEMY_URI"])


@pytest.fixture(scope="session")
def pyodbc_postgres_con() -> pyodbc.Connection:
    con_str = os.environ["PYODBC_URI"]
    with pyodbc.connect(con_str) as con:
        yield con


@pytest.fixture(scope="session")
def sql_adapter(
    pyodbc_postgres_con: pyodbc.Connection,
) -> adapter.PostgreSQLTableAdapter:
    table = adapter.inspect_table(
        con=pyodbc_postgres_con,
        table_name="employee",
        schema_name="hr",
    )
    return adapter.PostgreSQLTableAdapter(table=table)


def standardize_sql(sql: str) -> str:
    return " ".join(sql.split())
