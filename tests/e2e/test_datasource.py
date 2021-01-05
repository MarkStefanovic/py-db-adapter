import datetime
import itertools
import pathlib

import pyodbc

import py_db_adapter as pda


def check_customer2_table_in_sync(db_uri: str) -> None:
    with pyodbc.connect(db_uri) as con:
        with con.cursor() as cur:
            sql = "SELECT COUNT(*) FROM sales.customer"
            result = cur.execute(sql).fetchval()
            assert result > 0

            sql = "SELECT * FROM sales.customer"
            result = cur.execute(sql).fetchall()
            customer_values = {
                tuple(zip((col[0] for col in cur.description), row)) for row in result
            }
            sql = "SELECT * FROM sales.customer2"
            result = cur.execute(sql).fetchall()
            customer2_values = {
                tuple(zip((col[0] for col in cur.description), row)) for row in result
            }
    assert (
        customer2_values == customer_values
    ), f"\ncustomer:\n{sorted(customer_values)}\n\ncustomer2:\n{sorted(customer2_values)}"


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
        read_only=True,
    )
    with ds:
        assert ds.column_names == {
            "customer_first_name",
            "customer_id",
            "customer_last_name",
            "date_added",
            "date_updated",
        }


def test_sync_with_explicit_cols(
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
        read_only=True,
    )
    dest = src.copy(
        update={
            "table_name": "customer2",
            "read_only": False,
        }
    )
    with src, dest:
        # test add
        dest.sync(src=src, recreate=False)
        dest.commit()
        check_customer2_table_in_sync(postgres_pyodbc_db_uri)

        with src.db._connection._con.cursor() as cur:  # type: ignore
            # test update
            cur.execute(
                "UPDATE sales.customer SET customer_first_name = 'Frank' WHERE customer_first_name = 'Dan'"
            )
            cur.commit()
            dest.sync(src=src, recreate=False)
            dest.commit()
            check_customer2_table_in_sync(postgres_pyodbc_db_uri)

            # test delete
            cur.execute(
                "DELETE FROM sales.customer WHERE customer_first_name = 'Steve'"
            )
            cur.commit()
            dest.sync(src=src, recreate=False)
            dest.commit()
            check_customer2_table_in_sync(postgres_pyodbc_db_uri)

            rows = cur.execute("SELECT COUNT(*) FROM sales.customer").fetchval()
            assert rows == 8


def test_sync_with_default_cols(
    cache_dir: pathlib.Path, postgres_pyodbc_db_uri: str
) -> None:
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
        read_only=True,
    )
    dest = src.copy(
        update={
            "table_name": "customer2",
            "read_only": False,
        }
    )
    with src, dest:
        dest.sync(src=src, recreate=False)
        dest.commit()
        check_customer2_table_in_sync(postgres_pyodbc_db_uri)


def test_update_history_table(
    cache_dir: pathlib.Path, postgres_pyodbc_db_uri: str
) -> None:
    with pyodbc.connect(postgres_pyodbc_db_uri) as con:
        with con.cursor() as cur:
            sql = "SELECT COUNT(*) FROM sales.customer"
            original_rows = cur.execute(sql).fetchval()
            assert original_rows > 0

    ds = pda.postgres_pyodbc_datasource(
        db_name="test_db",
        db_uri=postgres_pyodbc_db_uri,
        cache_dir=cache_dir,
        schema_name="sales",
        table_name="customer",
        compare_cols=None,
        custom_pk_cols=None,
        max_batch_size=1000,
        read_only=False,
    )
    with ds:
        ds.update_history_table(recreate=False)
        ds.commit()

    with pyodbc.connect(postgres_pyodbc_db_uri) as con:
        with con.cursor() as cur:
            sql = "SELECT * FROM sales.customer_history"
            result = cur.execute(sql).fetchall()
            history = {
                tuple(zip((col[0] for col in cur.description), row)) for row in result
            }

    assert len(history) == original_rows


def test_update_history_after_source_rows_updated(
    cache_dir: pathlib.Path, postgres_pyodbc_db_uri: str
) -> None:
    with pyodbc.connect(postgres_pyodbc_db_uri) as con:
        with con.cursor() as cur:
            sql = "UPDATE sales.customer SET customer_last_name = 'Smithers' WHERE customer_first_name = 'Steve'"
            cur.execute(sql)
            cur.commit()
    ds = pda.postgres_pyodbc_datasource(
        db_name="test_db",
        db_uri=postgres_pyodbc_db_uri,
        cache_dir=cache_dir,
        schema_name="sales",
        table_name="customer",
        compare_cols=None,
        custom_pk_cols=None,
        max_batch_size=1000,
        read_only=False,
    )
    with ds:
        ds.update_history_table(recreate=False)
        ds.commit()

    with pyodbc.connect(postgres_pyodbc_db_uri) as con:
        with con.cursor() as cur:
            sql = "SELECT * FROM sales.customer_history WHERE customer_first_name = 'Steve'"
            result = cur.execute(sql).fetchall()
            updated_rows = [
                dict(zip((col[0] for col in cur.description), row)) for row in result
            ]
    assert len(updated_rows) == 2
    # fmt: off
    assert sum(row["valid_to"] == datetime.datetime(9999, 12, 31) for row in updated_rows) == 1
    original_row = next(row for row in updated_rows if row["valid_to"] != datetime.datetime(9999, 12, 31))
    new_row = next(row for row in updated_rows if row["valid_to"] == datetime.datetime(9999, 12, 31))
    assert original_row["valid_to"] + datetime.timedelta(microseconds=1) == new_row["valid_from"]
    # fmt: on


def test_update_history_after_source_rows_deleted(
    cache_dir: pathlib.Path, postgres_pyodbc_db_uri: str
) -> None:
    ds = pda.postgres_pyodbc_datasource(
        db_name="test_db",
        db_uri=postgres_pyodbc_db_uri,
        cache_dir=cache_dir,
        schema_name="sales",
        table_name="customer",
        compare_cols=None,
        custom_pk_cols=None,
        max_batch_size=1000,
        read_only=False,
    )
    with ds:
        ds.update_history_table(recreate=False)
        ds.commit()

    with pyodbc.connect(postgres_pyodbc_db_uri) as con:
        with con.cursor() as cur:
            sql = "DELETE FROM sales.customer WHERE customer_first_name = 'Dan'"
            cur.execute(sql)
            cur.commit()

    with ds:
        ds.update_history_table(recreate=False)
        ds.commit()

    # fmt: off
    with pyodbc.connect(postgres_pyodbc_db_uri) as con:
        with con.cursor() as cur:
            sql = "SELECT * FROM sales.customer_history WHERE customer_first_name = 'Dan'"
            result = cur.execute(sql).fetchall()
            deleted_rows = [
                dict(zip((col[0] for col in cur.description), row)) for row in result
            ]
    assert len(deleted_rows) == 1
    assert sum(row["valid_to"] == datetime.datetime(9999, 12, 31) for row in deleted_rows) == 0
    # fmt: on
