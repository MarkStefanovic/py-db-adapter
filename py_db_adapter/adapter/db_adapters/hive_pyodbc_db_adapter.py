import functools
import typing

from py_db_adapter import domain
from py_db_adapter.adapter import db_connections, sql_adapters

__all__ = ("HivePyodbcDbAdapter",)


class HivePyodbcDbAdapter(domain.DbAdapter):
    def __init__(
        self,
        *,
        con: db_connections.PyodbcConnection,
        hive_sql_adapter: sql_adapters.HiveSQLAdapter = sql_adapters.HiveSQLAdapter(),
    ):
        self._con = con
        self._hive_sql_adapter = hive_sql_adapter

    @property
    def _connection(self) -> domain.DbConnection:
        return self._con

    def fast_row_count(
        self, *, table_name: str, schema_name: typing.Optional[str] = None
    ) -> int:
        raise NotImplementedError

    @property
    def _sql_adapter(self) -> domain.SqlAdapter:
        return self._hive_sql_adapter

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
