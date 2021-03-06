import typing

import pyodbc

from py_db_adapter import domain
from py_db_adapter.adapter import sql_adapters

__all__ = ("PostgresAdapter",)


class PostgresAdapter(domain.DbAdapter):
    def __init__(
        self, *, sql_adapter: domain.SqlAdapter = sql_adapters.PostgreSQLAdapter()
    ):
        self.__sql_adapter = sql_adapter

    @property
    def _sql_adapter(self) -> domain.SqlAdapter:
        return self.__sql_adapter

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

        sql = self.__sql_adapter.table_exists(
            schema_name=schema_name, table_name=table_name
        )
        result = cur.execute(sql).fetchval()
        if result:
            if result == 0:
                return False
            elif result == 1:
                return True
            else:
                raise domain.exceptions.InvalidSqlGenerated(
                    sql=sql,
                    message=f"table_exists should return 0 for False, or 1 for True, but it returned {result!r}.",
                )
        return False
