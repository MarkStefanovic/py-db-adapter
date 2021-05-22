import pyodbc
import typing

from py_db_adapter import domain

logger = domain.root.getChild("copy_table")


__all__ = ("copy_table",)


def copy_table(
    *,
    cur: pyodbc.Cursor,
    dest_db_adapter: domain.DbAdapter,
    src_table: domain.Table,
    dest_table_name: str,
    dest_schema_name: typing.Optional[str],
    recreate: bool = False,
) -> typing.Tuple[domain.Table, bool]:
    """Copy a table's structure

    Does not copy the data.  That is what sync is for.
    """
    dest_table = src_table.copy_table(
        schema_name=dest_schema_name, table_name=dest_table_name
    )
    dest_table_exists = dest_db_adapter.table_exists(
        cur=cur, schema_name=dest_schema_name, table_name=dest_table_name
    )
    if not dest_table_exists:
        logger.debug(
            f"{dest_schema_name}.{dest_table_name} does not exist, so it will be created."
        )
        dest_db_adapter.create_table(cur=cur, table=dest_table)
        created = True
    elif recreate:
        logger.info(
            f"{dest_schema_name}.{dest_table_name} exists, but recreate = True, so the table will be recreated."
        )
        dest_db_adapter.drop_table(
            cur=cur, schema_name=dest_schema_name, table_name=dest_table_name
        )
        dest_db_adapter.create_table(cur=cur, table=dest_table)
        created = True
    else:
        logger.debug(f"{dest_schema_name}.{dest_table_name} already exists.")
        created = False

    return dest_table, created
