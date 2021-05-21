import typing

import pyodbc

from py_db_adapter import domain

__all__ = ("SqlServerAdapter",)


class SqlServerAdapter(domain.DbAdapter):
    def __init__(
        self, *, sql_adapter: domain.SqlAdapter = domain.SqlServerSQLAdapter()
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

        sql = self._sql_adapter.table_exists(
            schema_name=schema_name, table_name=table_name
        )
        result = cur.execute(sql).fetchone()
        if result:
            row_ct = result[0]
            if row_ct:
                return row_ct > 0
        return False
