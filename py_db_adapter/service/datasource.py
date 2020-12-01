from __future__ import annotations

import functools
import logging
import pathlib
import typing

import pydantic

from py_db_adapter import domain, adapter

__all__ = ("Datasource", "postgres_pyodbc_datasource", "read_only_hive_datasource")

logger = logging.getLogger(__name__)


class Datasource(pydantic.BaseModel):
    cache_dir: typing.Optional[pathlib.Path]
    compare_cols: typing.Optional[typing.Set[str]]
    db: adapter.DbAdapter
    max_batch_size: int
    pk_cols: typing.Optional[typing.Set[str]]
    read_only: bool
    schema_name: typing.Optional[str]
    table_name: str

    class Config:
        allow_mutation = False
        anystr_strip_whitespace = True
        arbitrary_types_allowed = True  # needed to handle adapter.DbAdapter
        min_anystr_length = 1

    @property
    def column_names(self) -> typing.Set[str]:
        return {col.column_name for col in self._table.columns}

    def copy_table(self, /, ds: Datasource) -> bool:
        if self.read_only:
            raise domain.exceptions.DatabaseIsReadOnly()

        if not self.db.sql_adapter.table_exists(
            schema_name=self.schema_name, table_name=self.table_name
        ):
            logger.debug(
                f"{self.schema_name}.{self.table_name} does not exist, so it will be created."
            )
            dest_table = ds._table.copy(
                update={
                    "schema_name": self.schema_name,
                    "table_name": self.table_name,
                }
            )
            sql = self.db.sql_adapter.definition(dest_table)
            self.db.connection.execute(sql)
            logger.debug(f"{self.schema_name}.{self.table_name} was created.")
            return True
        else:
            logger.debug(f"{self.schema_name}.{self.table_name} already exists.")
            return False

    @property
    def fast_row_count(self) -> typing.Optional[int]:
        return self.db.fast_row_count(
            schema_name=self.schema_name, table_name=self.table_name
        )

    @property
    def row_count(self) -> int:
        repo = self._create_repo()
        return repo.row_count()

    @property
    def exists(self) -> bool:
        return self.db.table_exists(
            schema_name=self.schema_name, table_name=self.table_name
        )

    def upsert(
        self,
        *,
        src: Datasource,
        add: bool = True,
        update: bool = True,
        delete: bool = True,
    ) -> None:
        if self.read_only:
            raise domain.exceptions.DatabaseIsReadOnly()

        self.copy_table(src)

        if self.pk_cols:
            pk_cols: typing.Set[str] = self.pk_cols
        elif src.pk_cols:
            pk_cols = src.pk_cols
        elif self._table.pk_cols:
            pk_cols = self._table.pk_cols
        elif src._table.pk_cols:
            pk_cols = src._table.pk_cols
        else:
            raise domain.exceptions.MissingPrimaryKey(
                schema_name=self.schema_name, table_name=self.table_name
            )

        if self.compare_cols:
            compare_cols: typing.Set[str] = self.compare_cols
        elif src.compare_cols:
            compare_cols = src.compare_cols
        else:
            compare_cols = self.column_names - pk_cols

        src_repo = src._create_repo()
        dest_repo = self._create_repo()

        dest_rows = dest_repo.keys(True)
        if dest_rows.is_empty:
            logger.info(
                f"{self.table_name} is empty so the source rows will be fully loaded."
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

    def _create_repo(self) -> adapter.Repository:
        return adapter.Repository(
            db=self.db,
            table=self._table,
            change_tracking_columns=self.compare_cols,
            batch_size=self.max_batch_size,
            read_only=self.read_only,
        )

    @functools.cached_property
    def _table(self) -> domain.Table:
        return self.db.connection.inspect_table(
            schema_name=self.schema_name,
            table_name=self.table_name,
            pk_cols=self.pk_cols,
            cache_dir=self.cache_dir,
        )


def postgres_pyodbc_datasource(
    *,
    db_name: str,
    db_uri: str,
    table_name: str,
    schema_name: typing.Optional[str] = None,
    cache_dir: typing.Optional[pathlib.Path] = None,
    compare_cols: typing.Optional[typing.Set[str]] = None,
    custom_pk_cols: typing.Optional[typing.Set[str]] = None,
    max_batch_size: int = 1_000,
    read_only: bool = False,
) -> Datasource:
    sql_adapter = adapter.PostgreSQLAdapter()
    con = adapter.PyodbcConnection(
        db_name=db_name, fast_executemany=False, uri=db_uri, autocommit=False
    )
    db = adapter.PostgresPyodbcDbAdapter(con=con, postgres_sql_adapter=sql_adapter)
    return Datasource(
        db=db,
        schema_name=schema_name,
        table_name=table_name,
        cache_dir=cache_dir,
        read_only=read_only,
        pk_cols=custom_pk_cols,
        compare_cols=compare_cols,
        max_batch_size=max_batch_size,
    )


def read_only_hive_datasource(
    *,
    db_name: str,
    db_uri: str,
    table_name: str,
    schema_name: typing.Optional[str] = None,
    cache_dir: typing.Optional[pathlib.Path] = None,
    compare_cols: typing.Optional[typing.Set[str]] = None,
    custom_pk_cols: typing.Optional[typing.Set[str]] = None,
    max_batch_size: int = 1_000,
) -> Datasource:
    sql_adapter = adapter.HiveSQLAdapter()
    con = adapter.HivePyodbcConnection(db_name=db_name, uri=db_uri)
    db = adapter.HivePyodbcDbAdapter(con=con, hive_sql_adapter=sql_adapter)
    return Datasource(
        db=db,
        schema_name=schema_name,
        table_name=table_name,
        cache_dir=cache_dir,
        read_only=True,
        max_batch_size=max_batch_size,
        pk_cols=custom_pk_cols,
        compare_cols=compare_cols,
    )