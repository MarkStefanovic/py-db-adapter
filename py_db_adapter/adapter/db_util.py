import typing

import pyodbc

from py_db_adapter import domain

__all__ = (
    "execute",
    "fetch_rows",
)

logger = domain.root.getChild(__name__)


def execute(
    *,
    cur: pyodbc.Cursor,
    sql: str,
    params: typing.Optional[typing.List[typing.Tuple[typing.Any, ...]]] = None,
) -> None:
    std_sql = domain.standardize_sql(sql)
    logger.debug(f"EXECUTE:\n\t{std_sql}\n\tparams={params}")
    positional_params = [tuple(param.values()) for param in params or {}]
    if params is None:
        cur.execute(std_sql)
    elif len(params) > 1:
        cur.executemany(std_sql, positional_params)
    else:
        cur.execute(std_sql, positional_params[0])


def fetch_rows(
    *,
    cur: pyodbc.Cursor,
    sql: str,
    params: typing.Optional[typing.List[typing.Tuple[typing.Any, ...]]] = None,
) -> domain.Rows:
    std_sql = domain.standardize_sql(sql)
    logger.debug(f"FETCH:\n\t{std_sql}\n\tparams={params}")
    positional_params = [tuple(param.values()) for param in params or {}]
    if params is None:
        result = cur.execute(std_sql)
    elif len(params) > 1:
        result = cur.executemany(std_sql, positional_params)
    else:
        result = cur.execute(std_sql, positional_params[0])

    column_names = [description[0] for description in cur.description]
    if rows := result.fetchall():
        return domain.Rows(column_names=column_names, rows=[tuple(row) for row in rows])
    else:
        return domain.Rows(column_names=column_names, rows=[])
