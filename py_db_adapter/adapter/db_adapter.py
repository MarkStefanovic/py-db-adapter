from __future__ import annotations

import abc
import types
import typing

from py_db_adapter import domain
from py_db_adapter.adapter import db_connection, sql_adapter

__all__ = ("DbAdapter",)


logger = domain.logger.getChild(__name__)


class DbAdapter(abc.ABC):
    """Intersection of DbConnection and SqlAdapter"""

    def close(self) -> None:
        self._connection.close()

    def commit(self) -> None:
        return self._connection.commit()

    def create_table(self, /, table: domain.Table) -> bool:
        if self.table_exists(
            table_name=table.table_name, schema_name=table.schema_name
        ):
            return True
        else:
            sql = self._sql_adapter.definition(table)
            self._connection.execute(sql=sql)
            logger.info(f"{table.schema_name}.{table.table_name} was created.")
            return False

    def drop_table(
        self, *, table_name: str, schema_name: typing.Optional[str] = None
    ) -> bool:
        if self.table_exists(table_name=table_name, schema_name=schema_name):
            sql = self._sql_adapter.drop(schema_name=schema_name, table_name=table_name)
            self._connection.execute(sql=sql)
            logger.info(f"{schema_name}.{table_name} was dropped.")
            return True
        else:
            return False

    def execute(
        self,
        *,
        sql: str,
        params: typing.Optional[typing.List[typing.Dict[str, typing.Any]]] = None,
    ) -> None:
        self._connection.execute(sql=sql, params=params)

    def fetch(
        self,
        *,
        sql: str,
        params: typing.Optional[typing.List[typing.Dict[str, typing.Any]]] = None,
    ) -> domain.Rows:
        return self._connection.fetch(sql=sql, params=params)

    @abc.abstractmethod
    def fast_row_count(
        self, *, table_name: str, schema_name: typing.Optional[str] = None
    ) -> int:
        raise NotImplementedError

    def open(self) -> None:
        self._connection.open()

    def row_count(self, *, table_name: str, schema_name: typing.Optional[str] = None) -> int:
        sql = self._sql_adapter.row_count(schema_name=schema_name, table_name=table_name)
        return typing.cast(int, self._connection.fetch(sql=sql).first_value())

    @abc.abstractmethod
    def table_exists(
        self, *, table_name: str, schema_name: typing.Optional[str] = None
    ) -> bool:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def _connection(self) -> db_connection.DbConnection:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def _sql_adapter(self) -> sql_adapter.SqlAdapter:
        raise NotImplementedError

    def __enter__(self) -> DbAdapter:
        self.open()
        return self

    def __exit__(
        self,
        exc_type: typing.Optional[typing.Type[BaseException]],
        exc_inst: typing.Optional[BaseException],
        exc_tb: typing.Optional[types.TracebackType],
    ) -> typing.Literal[False]:
        self.close()
        return False
