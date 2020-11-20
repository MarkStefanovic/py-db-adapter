from __future__ import annotations

import abc
import typing

__all__ = ("DbService",)


class DbService(abc.ABC):
    @abc.abstractmethod
    def copy_table(
        self,
        *,
        src_db: DbService,
        src_schema_name: typing.Optional[str],
        src_table_name: str,
        dest_schema_name: typing.Optional[str],
        dest_table_name: str,
        columns: typing.Optional[typing.Set[str]] = None,
    ) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def fast_row_count(
        self,
        *,
        table_name: str,
        schema_name: typing.Optional[str],
    ) -> int:
        raise NotImplementedError

    @abc.abstractmethod
    def inspect_table(self):
        raise NotImplementedError

    @abc.abstractmethod
    def table_exists(
        self,
        *,
        table_name: str,
        schema_name: typing.Optional[str],
    ) -> bool:
        raise NotImplementedError

    @abc.abstractmethod
    def upsert_table(
        self,
        *,
        src_db: DbService,
        src_schema_name: typing.Optional[str],
        src_table_name: str,
        dest_schema_name: typing.Optional[str],
        dest_table_name: str,
        compare_cols: typing.Optional[typing.Set[str]] = None,
        add: bool = True,
        update: bool = True,
        delete: bool = True,
    ) -> None:
        raise NotImplementedError
