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
    ds = pda.postgres_pyodbc_datasource(
        db_name="test_db",
        db_uri=postgres_pyodbc_db_uri,
        cache_dir=cache_dir,
        schema_name="sales",
        table_name="customer",
        compare_cols=None,
        custom_pk_cols=None,
        max_batch_size=1000,
        read_only=True
    )
    assert ds.column_names == {
        "customer_first_name",
        "customer_id",
        "customer_last_name",
        "date_added",
        "date_updated",
    }


def test_upsert_with_explicit_cols(
    cache_dir: pathlib.Path, postgres_pyodbc_db_uri: str
) -> None:
    src = pda.postgres_pyodbc_datasource(
        db_name="test_db",
        db_uri=postgres_pyodbc_db_uri,
        cache_dir=cache_dir,
        schema_name="sales",
        table_name="customer",
        compare_cols={"customer_first_name", "customer_last_name"},
        custom_pk_cols={"customer_id"},
        max_batch_size=1000,
        read_only=True
    )
    dest = src.copy(update={
        "table_name": "customer2",
        "read_only": False,
    })
    dest.upsert(
        src=src,
        add=True,
        update=True,
        delete=True,
    )
    check_customer2_table_in_sync(postgres_pyodbc_db_uri)


def test_upsert_with_default_cols(cache_dir: pathlib.Path, postgres_pyodbc_db_uri: str) -> None:
    with pyodbc.connect(postgres_pyodbc_db_uri) as con:
        with con.cursor() as cur:
            row_ct_sql = "SELECT COUNT(*) AS row_ct FROM sales.customer2"
            row_ct = cur.execute(row_ct_sql).fetchval()
            assert row_ct == 0

    src = pda.postgres_pyodbc_datasource(
        db_name="test_db",
        db_uri=postgres_pyodbc_db_uri,
        cache_dir=cache_dir,
        schema_name="sales",
        table_name="customer",
        compare_cols=None,
        custom_pk_cols=None,
        max_batch_size=1000,
        read_only=True
    )
    dest = src.copy(update={
        "table_name": "customer2",
        "read_only": False,
    })
    dest.upsert(
        src=src,
        add=True,
        update=True,
        delete=True,
    )
    check_customer2_table_in_sync(postgres_pyodbc_db_uri)
