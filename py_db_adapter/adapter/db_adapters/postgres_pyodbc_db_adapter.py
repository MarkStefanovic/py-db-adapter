import functools
import pathlib
import typing

from py_db_adapter.adapter import (
    db_adapter,
    sql_adapter,
    db_connection,
    sql_adapters,
    pyodbc_inspector,
    db_connections,
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
        table_name = self._sql_adapter.full_table_name(
            schema_name=schema_name, table_name=table_name
        )
        sql = f"""
            SELECT
                (reltuples / relpages) *
                (
                    pg_relation_size('{self.full_table_name}') / 
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

    def inspect_table(
        self,
        *,
        table_name: str,
        schema_name: typing.Optional[str] = None,
        custom_pk_cols: typing.Optional[typing.Set[str]] = None,
        cache_dir: typing.Optional[pathlib.Path] = None,
    ):
        if cache_dir is None:
            return pyodbc_inspector.pyodbc_inspect_table(
                con=self._con.handle,
                table_name=table_name,
                schema_name=schema_name,
                custom_pk_cols=custom_pk_cols,
            )
        else:
            return pyodbc_inspector.pyodbc_inspect_table_and_cache(
                con=self._con.handle,
                table_name=table_name,
                schema_name=schema_name,
                custom_pk_cols=custom_pk_cols,
                cache_dir=cache_dir,
            )

    @property
    def sql_adapter(self) -> sql_adapter.SqlAdapter:
        return self._sql_adapter

    def table_exists(
        self, *, table_name: str, schema_name: typing.Optional[str] = None
    ) -> bool:
        pass
