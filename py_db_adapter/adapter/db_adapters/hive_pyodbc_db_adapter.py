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
        hive_sql_adapter: sql_adapters.HiveSQLAdapter = sql_adapters.HiveSQLAdapter(),
    ):
        self._con = con
        self._hive_sql_adapter = hive_sql_adapter

    @property
    def _connection(self) -> db_connection.DbConnection:
        return self._con

    def fast_row_count(
        self, *, table_name: str, schema_name: typing.Optional[str] = None
    ) -> int:
        """A faster row-count method than .row_count(), but is only an estimate"""
        full_table_name = self._hive_sql_adapter.full_table_name(
            schema_name=schema_name, table_name=table_name
        )
        sql = f"DESCRIBE EXTENDED {full_table_name}"
        with self._connection as con:
            result = con.fetch(sql=sql)

        # sourcery skip: remove-unnecessary-else
        if result and not result.is_empty:
            for row in result.as_tuples():
                if row[0] == "Detailed Table Information":
                    num_rows_match = re.search(r".*, numRows=(\d+), .*", row[1])
                    if num_rows_match:
                        return int(num_rows_match.group(1))
            raise domain.exceptions.FastRowCountFailed(
                table_name=table_name,
                error_message=f"DESCRIBE EXTENDED {table_name} did not include numRows.",
            )
        else:
            raise domain.exceptions.TableDoesNotExist(
                table_name=table_name, schema_name=schema_name
            )

    @property
    def _sql_adapter(self) -> sql_adapter.SqlAdapter:
        return self._sql_adapter

    def table_exists(
        self, *, table_name: str, schema_name: typing.Optional[str] = None
    ) -> bool:
        return table_name in self.tables

    @functools.cached_property
    def tables(self) -> typing.Set[str]:
        sql = "SHOW TABLES"
        rows = self._con.fetch(sql=sql)
        if rows:
            return {row[0] for row in rows.as_tuples()}
        else:
            return set()
