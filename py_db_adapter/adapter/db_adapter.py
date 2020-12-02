import abc
import logging
import typing

from py_db_adapter import domain
from py_db_adapter.adapter import db_connection, sql_adapter

__all__ = ("DbAdapter",)


logger = logging.getLogger(__name__)


class DbAdapter(abc.ABC):
    """Intersection of DbConnection and SqlAdapter"""

    @property
    @abc.abstractmethod
    def connection(self) -> db_connection.DbConnection:
        raise NotImplementedError

    def create_table(self, /, table: domain.Table) -> bool:
        if self.table_exists(
            table_name=table.table_name, schema_name=table.schema_name
        ):
            return True
        else:
            sql = self.sql_adapter.definition(table)
            with self.connection as con:
                con.execute(sql)
                logger.info(f"{table.schema_name}.{table.table_name} was created.")
            return False

    def drop_table(
        self, *, table_name: str, schema_name: typing.Optional[str] = None
    ) -> bool:
        if self.table_exists(table_name=table_name, schema_name=schema_name):
            sql = self.sql_adapter.drop(schema_name=schema_name, table_name=table_name)
            with self.connection as con:
                con.execute(sql)
                logger.info(f"{schema_name}.{table_name} was dropped.")
            return True
        else:
            return False

    @abc.abstractmethod
    def fast_row_count(
        self, *, table_name: str, schema_name: typing.Optional[str] = None
    ) -> int:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def sql_adapter(self) -> sql_adapter.SqlAdapter:
        raise NotImplementedError

    @abc.abstractmethod
    def table_exists(
        self, *, table_name: str, schema_name: typing.Optional[str] = None
    ) -> bool:
        raise NotImplementedError
