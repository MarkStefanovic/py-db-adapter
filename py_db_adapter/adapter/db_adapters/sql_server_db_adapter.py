import typing

from py_db_adapter import domain
from py_db_adapter.adapter import db_connections, sql_adapters

__all__ = ("SqlServerPyodbcDbAdapter",)


class SqlServerPyodbcDbAdapter(domain.DbAdapter):
    def __init__(
        self,
        *,
        con: db_connections.PyodbcConnection,
        sql_server_sql_adapter: sql_adapters.SqlServerSQLAdapter = sql_adapters.SqlServerSQLAdapter(),
    ):
        self._con = con
        self._sql_server_sql_adapter = sql_server_sql_adapter

    @property
    def _connection(self) -> domain.DbConnection:
        return self._con

    @property
    def _sql_adapter(self) -> sql_adapters.SqlServerSQLAdapter:
        return self._sql_server_sql_adapter

    def table_exists(
        self, *, table_name: str, schema_name: typing.Optional[str] = None
    ) -> bool:
        if schema_name is None:
            raise domain.exceptions.SchemaIsRequired(
                f"A schema is required for PostgresPyodbcDbAdapter's table_exists method"
            )

        sql = self._sql_adapter.table_exists(
            schema_name=schema_name, table_name=table_name
        )
        result = self._connection.fetch(sql=sql, params=None)
        if result:
            row_ct = result.first_value()
            if row_ct:
                return row_ct > 0
        return False
