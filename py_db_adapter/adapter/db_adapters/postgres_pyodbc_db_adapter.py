import typing

from py_db_adapter import domain
from py_db_adapter.adapter import (
    db_adapter,
    db_connection,
    db_connections,
    sql_adapters,
)

__all__ = ("PostgresPyodbcDbAdapter",)


class PostgresPyodbcDbAdapter(db_adapter.DbAdapter):
    def __init__(
        self,
        *,
        con: db_connections.PyodbcConnection,
        postgres_sql_adapter: sql_adapters.PostgreSQLAdapter = sql_adapters.PostgreSQLAdapter(),
    ):
        self._con = con
        self._sql_adapter = postgres_sql_adapter

    @property
    def connection(self) -> db_connection.DbConnection:
        return self._con

    def fast_row_count(
        self, *, table_name: str, schema_name: typing.Optional[str] = None
    ) -> typing.Optional[int]:
        if schema_name is None:
            raise domain.exceptions.SchemaIsRequired(
                f"A schema is required for PostgresPyodbcDbAdapter's fast_row_count method"
            )

        sql = """
            SELECT n_live_tup
            FROM   pg_stat_all_tables
            WHERE  schemaname = ?
            AND    relname = ?
        """
        result = self.connection.execute(
            sql, [{"schema_name": schema_name, "table_name": table_name}]
        )

        if result is None or result.is_empty:
            return None

        row_ct = result.first_value()

        if row_ct != 0:
            return row_ct

        result = self.connection.execute(
            f'SELECT COUNT(*) AS row_ct FROM "{schema_name}"."{table_name}"'
        )
        if result is None or result.is_empty:
            return None

        return result.first_value()

    @property
    def sql_adapter(self) -> sql_adapters.PostgreSQLAdapter:
        return self._sql_adapter

    def table_exists(
        self, *, table_name: str, schema_name: typing.Optional[str] = None
    ) -> bool:
        if schema_name is None:
            raise domain.exceptions.SchemaIsRequired(
                f"A schema is required for PostgresPyodbcDbAdapter's table_exists method"
            )

        sql = f"""
           SELECT COUNT(*) AS ct
           FROM   information_schema.tables 
           WHERE  table_schema = ?
           AND    table_name   = ?
        """
        result = self.connection.execute(
            sql, [{"schema_name": schema_name, "table_name": table_name}]
        )
        if result:
            row_ct = result.first_value()
            if row_ct:
                return row_ct > 0
        return False