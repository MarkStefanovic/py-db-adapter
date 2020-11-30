from __future__ import annotations

import functools
import logging
import pathlib
import typing

import pydantic

from py_db_adapter import domain, adapter

__all__ = ("DbService", "postgres_pyodbc_service", "read_only_hive_service")

logger = logging.getLogger(__name__)


class DbService(pydantic.BaseModel):
    db: adapter.DbAdapter
    cache_dir: typing.Optional[pathlib.Path] = None
    read_only: bool = False

    class Config:
        allow_mutation = False
        anystr_strip_whitespace = True
        min_anystr_length = 1
        # underscore_attrs_are_private = True

    # @pydantic.validator("db_name", "db_uri")
    # def db_name_is_required(cls, v: str):
    #     if v:
    #         return v
    #     else:
    #         raise ValueError(f"Value cannot be blank.")

    def copy_table_if_not_exists(
        self,
        *,
        src_db: DbService,
        src_schema_name: typing.Optional[str],
        src_table_name: str,
        dest_schema_name: typing.Optional[str],
        dest_table_name: str,
    ) -> bool:
        if self.read_only:
            raise domain.exceptions.DatabaseIsReadOnly()

        if not self.db.sql_adapter.table_exists(
            schema_name=dest_table.schema_name, table_name=dest_table.table_name
        ):
            logger.debug(
                f"{dest_schema_name}.{dest_table_name} does not exist, so it will be created."
            )
            src_table = src_db._inspect_table(
                table_name=src_table_name,
                schema_name=src_schema_name,
                custom_pk_cols=pk_columns,
            )
            dest_table = src_table.copy(
                schema_name=dest_schema_name,
                table_name=dest_table_name,
            )
            sql = self.db.sql_adapter.definition(dest_table)
            self.db.connection.execute(sql)
            logger.debug(f"{dest_schema_name}.{dest_table_name} was created.")
            return True
        else:
            logger.debug(f"{dest_schema_name}.{dest_table_name} already exists.")
            return False

    def fast_row_count(
        self, *, schema_name: typing.Optional[str], table_name: str
    ) -> typing.Optional[int]:
        return self.db.fast_row_count(schema_name=schema_name, table_name=table_name)

    def row_count(self, *, schema_name: typing.Optional[str], table_name: str) -> int:
        repo = self._create_repo()

    def table_exists(
        self, *, schema_name: typing.Optional[str], table_name: str
    ) -> bool:
        return self.db.table_exists(schema_name=schema_name, table_name=table_name)

    def upsert_table(
        self,
        *,
        src_db: DbService,
        src_table: domain.Table,
        dest_table: domain.Table,
        add: bool = True,
        update: bool = True,
        delete: bool = True,
        batch_size: int = 1_000,
    ) -> None:
        if self.read_only:
            raise domain.exceptions.DatabaseIsReadOnly()

        self.copy_table_if_not_exists(
            src_db=src_db,
            src_schema_name=src_schema_name,
            src_table_name=src_table_name,
            dest_schema_name=dest_schema_name,
            dest_table_name=dest_table_name,
            pk_columns=pk_cols,
        )

        # sourcery skip: hoist-if-from-if
        if pk_cols is None or compare_cols is None:
            src_table = src_db._inspect_table(
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

        src_repo = src_db._create_repo(
            schema_name=src_schema_name,
            table_name=src_table_name,
            change_tracking_columns=compare_cols,
            pk_columns=pk_cols,
            batch_size=batch_size,
        )
        dest_repo = self._create_repo(
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

    def _create_repo(
        self,
        *,
        table: domain.Table,
        batch_size: int = 1_000,
    ) -> adapter.Repository:
        table = self._inspect_table(
            table_name=table_name,
            schema_name=schema_name,
            custom_pk_cols=pk_columns,
        )
        return adapter.Repository(
            db=self.db,
            table=table,
            change_tracking_columns=change_tracking_columns,
            batch_size=batch_size,
        )

    @functools.lru_cache
    def _inspect_table(
        self,
        *,
        schema_name: typing.Optional[str],
        table_name: str,
        custom_pk_cols: typing.Set[str],
    ) -> domain.Table:
        return self.db.connection.inspect_table(
            schema_name=schema_name,
            table_name=table_name,
            custom_pk_cols=custom_pk_cols,
            cache_dir=self.cache_dir,
        )


def postgres_pyodbc_service(
    *,
    db_name: str,
    db_uri: str,
    cache_dir: typing.Optional[pathlib.Path] = None,
    read_only: bool = False,
) -> DbService:
    sql_adapter = adapter.PostgreSQLAdapter()
    con = adapter.PyodbcConnection(db_name=db_name, fast_executemany=False, uri=db_uri)
    db = adapter.PostgresPyodbcDbAdapter(con=con, postgres_sql_adapter=sql_adapter)
    return DbService(db=db, cache_dir=cache_dir, read_only=read_only)


def read_only_hive_service(
    *, db_name: str, db_uri: str, cache_dir: typing.Optional[pathlib.Path] = None
) -> DbService:
    sql_adapter = adapter.HiveSQLAdapter()
    con = adapter.HivePyodbcConnection(db_name=db_name, uri=db_uri)
    db = adapter.HivePyodbcDbAdapter(con=con, hive_sql_adapter=sql_adapter)
    return DbService(db=db, cache_dir=cache_dir, read_only=True)
