import pathlib

import pyodbc
import typing

from py_db_adapter import adapter, domain

logger = domain.root_logger.getChild("copy_table")


__all__ = ("copy_table",)


def copy_table(
    # fmt: off
    *,
    src_cur: pyodbc.Cursor,
    dest_cur: pyodbc.Cursor,
    dest_db_adapter: domain.DbAdapter,
    src_table_name: str,
    src_schema_name: typing.Optional[str],
    dest_table_name: str,
    dest_schema_name: typing.Optional[str],
    pk_cols: typing.Optional[typing.List[str]] = None,  # None = inspect to find out
    include_cols: typing.Optional[typing.List[str]] = None,  # None = inspect to find out
    recreate: bool = False,
    cache_dir: typing.Optional[pathlib.Path] = None,
    # fmt: on
) -> typing.Tuple[domain.Table, bool]:
    """Copy a table's structure

    Does not copy the data.  That is what sync is for.
    """
    src_table = adapter.inspect_table(
        cur=src_cur,
        table_name=src_table_name,
        schema_name=src_schema_name,
        pk_cols=pk_cols,
        include_cols=include_cols,
        cache_dir=cache_dir,
    )
    dest_table = src_table.copy_table(
        schema_name=dest_schema_name, table_name=dest_table_name
    )
    dest_table_exists = dest_db_adapter.table_exists(
        cur=dest_cur, schema_name=dest_schema_name, table_name=dest_table_name
    )
    if not dest_table_exists:
        logger.debug(
            f"{dest_schema_name}.{dest_table_name} does not exist, so it will be created."
        )
        dest_db_adapter.create_table(cur=dest_cur, table=dest_table)
        created = True
    elif recreate:
        logger.info(
            f"{dest_schema_name}.{dest_table_name} exists, but recreate = True, so the table will be recreated."
        )
        dest_db_adapter.drop_table(
            cur=dest_cur, schema_name=dest_schema_name, table_name=dest_table_name
        )
        dest_db_adapter.create_table(cur=dest_cur, table=dest_table)
        created = True
    else:
        logger.debug(f"{dest_schema_name}.{dest_table_name} already exists.")
        created = False

    return dest_table, created
