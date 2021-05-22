import functools
import typing

import pyodbc

from py_db_adapter import domain
from py_db_adapter.adapter import sql_adapters

__all__ = ("HiveAdapter",)


class HiveAdapter(domain.DbAdapter):
    def __init__(
        self, *, sql_adapter: domain.SqlAdapter = sql_adapters.HiveSQLAdapter()
    ):
        self.__sql_adapter = sql_adapter

    def fast_row_count(
        self,
        *,
        cur: pyodbc.Cursor,
        table_name: str,
        schema_name: typing.Optional[str] = None
    ) -> int:
        raise NotImplementedError

    @property
    def _sql_adapter(self) -> domain.SqlAdapter:
        return self.__sql_adapter

    def table_exists(
        self,
        *,
        cur: pyodbc.Cursor,
        table_name: str,
        schema_name: typing.Optional[str] = None
    ) -> bool:
        return table_name in self.tables

    @functools.cached_property  # type: ignore
    def tables(self, *, cur: pyodbc.Cursor) -> typing.Set[str]:
        sql = "SHOW TABLES"
        rows = cur.execute(sql=sql).fetchall()
        if rows:
            return {row[0] for row in rows}
        else:
            return set()
