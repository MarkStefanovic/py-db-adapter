from __future__ import annotations

import abc
import pathlib
import typing

from py_db_adapter import domain

__all__ = ("DbConnection",)


class DbConnection(abc.ABC):
    @abc.abstractmethod
    def commit(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def execute(
        self,
        sql: str,
        params: typing.Optional[typing.List[typing.Dict[str, typing.Any]]] = None,
    ) -> typing.Optional[domain.Rows]:
        raise NotImplementedError

    @abc.abstractmethod
    def inspect_table(
        self,
        *,
        table_name: str,
        schema_name: typing.Optional[str] = None,
        custom_pk_cols: typing.Optional[typing.Set[str]] = None,
        cache_dir: typing.Optional[pathlib.Path] = None,
    ):
        raise NotImplementedError

    def parameter_placeholder(self, /, column_name: str) -> str:
        return "?"

    @abc.abstractmethod
    def rollback(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def __enter__(self) -> DbConnection:
        raise NotImplementedError

    @abc.abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        raise NotImplementedError
