import typing

from py_db_adapter import domain
from py_db_adapter.adapter import (
    db_adapter,
    db_connection,
    db_connections,
    sql_adapter,
    sql_adapters,
)


class PostgresPyodbcDbAdapter(db_adapter.DbAdapter):
    def __init__(
        self,
        *,
        con: db_connections.PyodbcConnection,
        postgres_sql_adapter: typing.Optional[sql_adapters.PostgreSQLAdapter] = None,
    ):
        self._con = con
        if sql_adapter:
            self._sql_adapter = postgres_sql_adapter
        else:
            self._sql_adapter = sql_adapters.PostgreSQLAdapter()

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

        sql = f"""
            SELECT
                (reltuples / relpages)
                * (
                    pg_relation_size('{schema_name}.{table_name}') / 
                    current_setting('block_size')::INTEGER
                ) AS rows
            FROM pg_class
            WHERE
                relname = '{table_name}'
        """
        result = self.connection.execute(sql)
        if result.is_empty:
            return None
        else:
            return result.first_value()

    @property
    def sql_adapter(self) -> sql_adapter.SqlAdapter:
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
           WHERE  table_schema = '{schema_name}'
           AND    table_name   = '{table_name}'
        );
        """
        result = self._con.execute(sql).first_value()
        return result > 0
