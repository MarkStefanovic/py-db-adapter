import typing

import pyodbc

from py_db_adapter import adapter
from py_db_adapter.service import sql_generator
import sqlalchemy as sa


def chunk_items(
    items: typing.Collection[typing.Any], n: int
) -> typing.List[typing.Iterable[typing.Any]]:
    items = list(items)
    return [items[i : i + n] for i in range(0, len(items), n)]


def get_keys(
    *,
    sql_adapter: adapter.SqlTableAdapter,
    con_or_engine: typing.Union[pyodbc.Connection, sa.engine.Engine],
    additional_cols: typing.List[str],
) -> typing.List[typing.Dict[str, typing.Any]]:
    sql = sql_generator.get_keys(
        sql_adapter=sql_adapter,
        additional_cols=additional_cols,
    )
    if isinstance(con_or_engine, pyodbc.Connection):
        with con_or_engine.cursor() as cur:
            result = cur.execute(sql).fetchall()
            column_names = [col[0] for col in cur.description]
            return [dict(zip(column_names, row)) for row in result]
    else:
        with con_or_engine.begin() as con:
            result = con.execute(sa.text(sql)).fetchall()
            return [dict(row) for row in result]


def _compare_keys(
    *,
    pk_cols: typing.Set[str],
    compare_cols: typing.List[str],
    src_rows: typing.List[typing.Dict[str, typing.Any]],
    dest_rows: typing.List[typing.Dict[str, typing.Any]],
) -> typing.Dict[
    str,
    typing.Dict[
        typing.Tuple[typing.Tuple[str, typing.Any], ...],
        typing.Tuple[typing.Tuple[str, typing.Any], ...],
    ],
]:
    src_hashes = {
        tuple(sorted((k, v) for k, v in row.items() if k in pk_cols)): tuple(
            sorted((k, v) for k, v in row.items() if k in compare_cols)
        )
        for row in src_rows
    }
    dest_hashes = {
        tuple(sorted((k, v) for k, v in row.items() if k in pk_cols)): tuple(
            sorted((k, v) for k, v in row.items() if k in compare_cols)
        )
        for row in dest_rows
    }
    src_key_set = set(src_hashes.keys())
    dest_key_set = set(dest_hashes.keys())
    added = {k: src_hashes[k] for k in (src_key_set - dest_key_set)}
    deleted = {k: src_hashes.get(k, tuple()) for k in (dest_key_set - src_key_set)}
    updates = {
        k: src_hashes.get(k, tuple())
        for k in src_key_set
        if k not in added
        and k not in deleted
        and src_hashes.get(k, tuple()) != dest_hashes.get(k, tuple())
    }
    return {
        "added": added,
        "deleted": deleted,
        "updated": updates,
    }


def get_changes(
    src_con_or_engine: typing.Union[pyodbc.Connection, sa.engine.Engine],
    dest_con_or_engine: typing.Union[pyodbc.Connection, sa.engine.Engine],
    src_sql_adapter: adapter.SqlTableAdapter,
    dest_sql_adapter: adapter.SqlTableAdapter,
    compare_cols: typing.List[str],
) -> typing.Dict[
    str,
    typing.Dict[
        typing.Tuple[typing.Tuple[str, typing.Any], ...],
        typing.Tuple[typing.Tuple[str, typing.Any], ...],
    ],
]:
    pk_cols = src_sql_adapter.table_metadata.primary_key_column_names
    src_rows = get_keys(
        sql_adapter=src_sql_adapter,
        con_or_engine=src_con_or_engine,
        additional_cols=compare_cols,
    )
    dest_rows = get_keys(
        sql_adapter=dest_sql_adapter,
        con_or_engine=dest_con_or_engine,
        additional_cols=compare_cols,
    )
    return _compare_keys(
        pk_cols=pk_cols,
        compare_cols=compare_cols,
        src_rows=src_rows,
        dest_rows=dest_rows,
    )
