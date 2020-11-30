from __future__ import annotations

import abc
import logging
import pathlib
import typing

from py_db_adapter import domain, adapter

__all__ = ("DbService",)

logger = logging.getLogger(__name__)


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

    def copy_table_if_not_exists(
        self,
        *,
        src_db: DbService,
        src_schema_name: typing.Optional[str],
        src_table_name: str,
        dest_schema_name: typing.Optional[str],
        dest_table_name: str,
        pk_columns: typing.Optional[typing.Set[str]] = None,
    ) -> None:
        if not self.db.sql_adapter.table_exists(
            schema_name=dest_schema_name, table_name=dest_table_name
        ):
            logger.debug(f"{dest_schema_name}.{dest_table_name} does not exist, so it will be created.")
            src_table = src_db.con.inspect_table(
                table_name=src_table_name,
                schema_name=src_schema_name,
                custom_pk_cols=pk_columns,
                cache_dir=self.cache_dir,
            )
            dest_table = src_table.copy(
                new_schema_name=dest_schema_name,
                new_table_name=dest_table_name,
            )
            sql = self.db.sql_adapter.definition(dest_table)
            self.db.connection.execute(sql)
            logger.debug(f"{dest_schema_name}.{dest_table_name} was created.")
        else:
            logger.debug(f"{dest_schema_name}.{dest_table_name} already exists.")

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

    def upsert_table(
        self,
        *,
        src_db: DbService,
        src_schema_name: typing.Optional[str],
        src_table_name: str,
        dest_schema_name: typing.Optional[str],
        dest_table_name: str,
        pk_cols: typing.Optional[typing.Set[str]] = None,
        compare_cols: typing.Optional[typing.Set[str]] = None,
        add: bool = True,
        update: bool = True,
        delete: bool = True,
        batch_size: int = 1_000,
    ) -> None:
        self.copy_table_if_not_exists(
            src_db=src_db,
            src_schema_name=src_schema_name,
            src_table_name=src_table_name,
            dest_schema_name=dest_schema_name,
            dest_table_name=dest_table_name,
            pk_columns=pk_cols
        )

        # sourcery skip: hoist-if-from-if
        if pk_cols is None or compare_cols is None:
            src_table = src_db.inspect_table(
                schema_name=src_schema_name, table_name=src_table_name
            )
            if pk_cols is None:
                pk_cols = src_table.primary_key_column_names
            if compare_cols is None:
                compare_cols = {
                    col
                    for col in src_table.column_names
                    if col not in src_table.primary_key_column_names
                }

        src_repo = src_db.create_repo(
            schema_name=src_schema_name,
            table_name=src_table_name,
            change_tracking_columns=compare_cols,
            pk_columns=pk_cols,
            batch_size=batch_size,
        )
        dest_repo = self.create_repo(
            schema_name=dest_schema_name,
            table_name=dest_table_name,
            change_tracking_columns=compare_cols,
            pk_columns=pk_cols,
            batch_size=batch_size,
        )

        dest_rows = dest_repo.keys(True)
        if dest_rows.is_empty:
            logger.info(
                f"{dest_table_name} is empty so the source rows will be fully loaded."
            )
            src_rows = src_repo.all()
            dest_repo.add(src_rows)
        else:
            src_rows = src_repo.keys(include_change_tracking_cols=True)
            dest_rows = dest_repo.keys(include_change_tracking_cols=True)
            changes = dest_rows.compare(
                rows=src_rows,
                key_cols=pk_cols,
                compare_cols=compare_cols,
                ignore_missing_key_cols=True,
                ignore_extra_key_cols=True,
            )
            common_cols = src_repo.table.column_names & dest_repo.table.column_names
            if changes.rows_added.row_count and add:
                new_rows = src_repo.fetch_rows_by_primary_key_values(
                    rows=changes.rows_added, cols=common_cols
                )
                dest_repo.add(new_rows)
            if changes.rows_deleted.row_count and delete:
                dest_repo.delete(changes.rows_deleted)
            if changes.rows_updated.row_count and update:
                updated_rows = src_repo.fetch_rows_by_primary_key_values(
                    rows=changes.rows_updated, cols=common_cols
                )
                dest_repo.update(updated_rows)
        self.db.connection.commit()
