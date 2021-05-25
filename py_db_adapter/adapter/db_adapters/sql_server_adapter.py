import typing

import pyodbc

from py_db_adapter import domain
from py_db_adapter.adapter import sql_adapters

__all__ = ("SqlServerAdapter",)


class SqlServerAdapter(domain.DbAdapter):
    def __init__(
        self, *, sql_adapter: domain.SqlAdapter = sql_adapters.SqlServerSQLAdapter()
    ):
        self.__sql_adapter = sql_adapter

    @property
    def _sql_adapter(self) -> domain.SqlAdapter:
        return self.__sql_adapter

    @property
    def fast_executemany_available(self) -> bool:
        return True

    def table_exists(
        self,
        *,
        cur: pyodbc.Cursor,
        table_name: str,
        schema_name: typing.Optional[str] = None,
    ) -> bool:
        if schema_name is None:
            raise domain.exceptions.SchemaIsRequired(
                f"A schema is required for PostgresPyodbcDbAdapter's table_exists method"
            )

        sql = self._sql_adapter.table_exists(
            schema_name=schema_name, table_name=table_name
        )
        result = cur.execute(sql).fetchval()
        if result:
            return result > 0
        return False
