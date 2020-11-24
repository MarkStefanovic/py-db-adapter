import abc

__all__ = ("DbAdapter",)

import pathlib

import typing

from py_db_adapter.adapter import db_connection, sql_adapter


class DbAdapter(abc.ABC):
    @property
    @abc.abstractmethod
    def connection(self) -> db_connection.DbConnection:
        raise NotImplementedError

    @abc.abstractmethod
    def fast_row_count(
        self, *, table_name: str, schema_name: typing.Optional[str] = None
    ) -> typing.Optional[int]:
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
