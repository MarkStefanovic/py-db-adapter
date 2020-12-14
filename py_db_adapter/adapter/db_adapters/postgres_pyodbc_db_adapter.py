import typing

from py_db_adapter import domain
from py_db_adapter.adapter import db_connections, sql_adapters

__all__ = ("PostgresPyodbcDbAdapter",)


class PostgresPyodbcDbAdapter(domain.DbAdapter):
    def __init__(
        self,
        *,
        con: db_connections.PyodbcConnection,
        postgres_sql_adapter: sql_adapters.PostgreSQLAdapter = sql_adapters.PostgreSQLAdapter(),
    ):
        self._con = con
        self._postgres_sql_adapter = postgres_sql_adapter

    @property
    def _connection(self) -> domain.DbConnection:
        return self._con

    @property
    def _sql_adapter(self) -> sql_adapters.PostgreSQLAdapter:
        return self._postgres_sql_adapter

    def table_exists(
        self, *, table_name: str, schema_name: typing.Optional[str] = None
    ) -> bool:
        if schema_name is None:
            raise domain.exceptions.SchemaIsRequired(
                f"A schema is required for PostgresPyodbcDbAdapter's table_exists method"
            )

        sql = self._postgres_sql_adapter.table_exists(
            schema_name=schema_name, table_name=table_name
        )
        result = self._connection.fetch(sql=sql, params=None)
        if result:
            flag = result.first_value()
            if flag == 0:
                return False
            elif flag == 1:
                return True
            else:
                raise domain.exceptions.InvalidSqlGenerated(
                    sql=sql,
                    message=f"table_exists should return 0 for False, or 1 for True, but it returned {flag!r}.",
                )
        return False
