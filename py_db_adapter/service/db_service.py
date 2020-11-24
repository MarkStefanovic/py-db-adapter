from __future__ import annotations

import abc
import pathlib
import typing

from py_db_adapter import domain, adapter

__all__ = ("DbService",)


class DbService(abc.ABC):
    @property
    @abc.abstractmethod
    def cache_dir(self) -> typing.Optional[pathlib.Path]:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def con(self) -> adapter.DbConnection:
        raise NotImplementedError

    def create_repo(
        self,
        *,
        schema_name: typing.Optional[str],
        table_name: str,
        change_tracking_columns: typing.Set[str],
        pk_columns: typing.Optional[typing.Set[str]] = None,
        batch_size: int = 1_000,
    ) -> adapter.Repository:
        table = self.con.inspect_table(
            table_name=table_name,
            schema_name=schema_name,
            custom_pk_cols=pk_columns,
            cache_dir=self.cache_dir,
        )
        return adapter.Repository(
            db=self.db,
            table=table,
            change_tracking_columns=change_tracking_columns,
            batch_size=batch_size,
        )

    @property
    @abc.abstractmethod
    def db(self) -> adapter.DbAdapter:
        raise NotImplementedError

    @abc.abstractmethod
    def fast_row_count(
        self, *, schema_name: typing.Optional[str], table_name: str
    ) -> typing.Optional[int]:
        raise NotImplementedError

    @abc.abstractmethod
    def inspect_table(
        self, *, schema_name: typing.Optional[str], table_name: str
    ) -> domain.Table:
        raise NotImplementedError

    @abc.abstractmethod
    def table_exists(
        self, *, schema_name: typing.Optional[str], table_name: str
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
