from __future__ import annotations

import abc
import pathlib
import types
import typing

from py_db_adapter import domain

__all__ = ("DbConnection",)


class DbConnection(abc.ABC):
    @abc.abstractmethod
    def close(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def commit(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def execute(
        self,
        *,
        sql: str,
        params: typing.Optional[typing.List[typing.Dict[str, typing.Any]]] = None,
    ) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def fetch(
        self,
        *,
        sql: str,
        params: typing.Optional[typing.List[typing.Dict[str, typing.Any]]] = None,
    ) -> domain.Rows:
        raise NotImplementedError

    @abc.abstractmethod
    def inspect_table(
        self,
        *,
        table_name: str,
        schema_name: typing.Optional[str] = None,
        pk_cols: typing.Optional[typing.Set[str]] = None,
        cache_dir: typing.Optional[pathlib.Path] = None,
        sync_cols: typing.Optional[typing.Set[str]] = None,
    ) -> domain.Table:
        raise NotImplementedError

    @abc.abstractmethod
    def open(self) -> None:
        raise NotImplementedError

    def parameter_placeholder(self, /, column_name: str) -> str:
        return "?"

    @abc.abstractmethod
    def rollback(self) -> None:
        raise NotImplementedError

    def __enter__(self) -> DbConnection:
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
