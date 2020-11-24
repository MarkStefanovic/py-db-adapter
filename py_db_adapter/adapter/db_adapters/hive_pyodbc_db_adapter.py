import functools
import re
import typing

from py_db_adapter import domain
from py_db_adapter.adapter import (
    db_adapter,
    db_connection,
    db_connections,
    sql_adapter,
    sql_adapters,
)

__all__ = ("HivePyodbcDbAdapter",)


class HivePyodbcDbAdapter(db_adapter.DbAdapter):
    def __init__(
        self,
        *,
        con: db_connections.PyodbcConnection,
        hive_sql_adapter: typing.Optional[sql_adapters.HiveSQLAdapter] = None,
    ):
        self._con = con
        if sql_adapter:
            self._sql_adapter = hive_sql_adapter
        else:
            self._sql_adapter = sql_adapters.HiveSQLAdapter()

    @property
    def connection(self) -> db_connection.DbConnection:
        return self._con

    def fast_row_count(
        self, *, table_name: str, schema_name: typing.Optional[str] = None
    ) -> int:
        """A faster row-count method than .row_count(), but is only an estimate"""
        full_table_name = self._sql_adapter.full_table_name(
            schema_name=schema_name, table_name=table_name
        )
        sql = f"DESCRIBE EXTENDED {full_table_name}"
        result = self._con.execute(sql)
        # sourcery skip: remove-unnecessary-else
        if result.is_empty:
            raise domain.exceptions.TableDoesNotExist(
                f"No results were returned from {sql!r}."
            )
        else:
            for row in result.as_tuples():
                if row[0] == "Detailed Table Information":
                    num_rows_match = re.search(".*, numRows=(\d+), .*", row[1])
                    if num_rows_match:
                        return int(num_rows_match.group(1))
            raise domain.exceptions.FastRowCountFailed()

    @property
    def sql_adapter(self) -> sql_adapter.SqlAdapter:
        return self._sql_adapter

    def table_exists(
        self, *, table_name: str, schema_name: typing.Optional[str] = None
    ) -> bool:
        return table_name in self.tables

    @functools.cached_property
    def tables(self) -> typing.Set[str]:
        sql = "SHOW TABLES"
        rows = self._con.execute(sql)
        return {row[0] for row in rows.as_tuples()}
