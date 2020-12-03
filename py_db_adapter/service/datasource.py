from __future__ import annotations

import pathlib
import types
import typing

import pydantic

from py_db_adapter import domain, adapter

__all__ = ("Datasource", "postgres_pyodbc_datasource", "read_only_hive_datasource")

logger = domain.logger.getChild("Datasource")


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

    def commit(self) -> None:
        self.db.commit()

    def copy_table(
        self, *, table: domain.Table, recreate: bool = False
    ) -> typing.Tuple[domain.Table, bool]:
        if self.read_only:
            raise domain.exceptions.DatabaseIsReadOnly()

        dest_table = table.copy(
            update={
                "schema_name": self.schema_name,
                "table_name": self.table_name,
            }
        )
        dest_table_exists = self.db.table_exists(
            schema_name=self.schema_name, table_name=self.table_name
        )
        if not dest_table_exists:
            logger.debug(
                f"{self.schema_name}.{self.table_name} does not exist, so it will be created."
            )
            self.db.create_table(dest_table)
            created = True
        elif recreate:
            logger.info(
                f"{self.schema_name}.{self.table_name} exists, but recreate = True, so the table will be recreated."
            )
            self.db.drop_table(schema_name=self.schema_name, table_name=self.table_name)
            self.db.create_table(dest_table)
            created = True
        else:
            logger.debug(f"{self.schema_name}.{self.table_name} already exists.")
            created = False

        return dest_table, created

    @property
    def fast_row_count(self) -> typing.Optional[int]:
        return self.db.fast_row_count(
            schema_name=self.schema_name, table_name=self.table_name
        )

    @property
    def row_count(self) -> int:
        repo = self._create_repo(self._table)
        return repo.row_count()

    @property
    def exists(self) -> bool:
        return self.db.table_exists(
            schema_name=self.schema_name, table_name=self.table_name
        )

    def sync(
        self,
        *,
        src: Datasource,
        add: bool = True,
        update: bool = True,
        delete: bool = True,
        recreate: bool = False,
    ) -> None:
        if self.read_only:
            raise domain.exceptions.DatabaseIsReadOnly()

        dest_table, created = self.copy_table(table=src._table, recreate=recreate)

        if src.pk_cols:
            pk_cols = src.pk_cols
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
            compare_cols = dest_table.column_names - pk_cols

        src_repo = src._create_repo(src._table)
        dest_repo = self._create_repo(dest_table)

        dest_rows = dest_repo.keys(include_change_tracking_cols=True)
        if dest_rows.is_empty:
            logger.info(
                f"{self.table_name} is empty so the source rows will be fully loaded."
            )
            src_rows = src_repo.all()
            dest_repo.add(src_rows)
        else:
            src_rows = src_repo.keys(include_change_tracking_cols=True)
            changes = dest_rows.compare(
                rows=src_rows,
                key_cols=pk_cols,
                compare_cols=compare_cols,
                ignore_missing_key_cols=True,
                ignore_extra_key_cols=True,
            )
            if changes.rows_added.row_count and add:
                new_rows = src_repo.fetch_rows_by_primary_key_values(
                    rows=changes.rows_added, cols=src._table.column_names
                )
                dest_repo.add(new_rows)
            if changes.rows_deleted.row_count and delete:
                dest_repo.delete(changes.rows_deleted)
            if changes.rows_updated.row_count and update:
                updated_rows = src_repo.fetch_rows_by_primary_key_values(
                    rows=changes.rows_updated, cols=src._table.column_names
                )
                dest_repo.update(updated_rows)

    def _create_repo(self, /, table: domain.Table) -> adapter.Repository:
        return adapter.Repository(
            db=self.db,
            table=table,
            change_tracking_columns=self.compare_cols,
            batch_size=self.max_batch_size,
            read_only=self.read_only,
        )

    @property
    def _table(self) -> domain.Table:
        return self.db.inspect_table(
            schema_name=self.schema_name,
            table_name=self.table_name,
            pk_cols=self.pk_cols,
            cache_dir=self.cache_dir,
        )

    def __enter__(self) -> Datasource:
        self.db.open()
        return self

    def __exit__(
        self,
        exc_type: typing.Optional[typing.Type[BaseException]],
        exc_inst: typing.Optional[BaseException],
        exc_tb: typing.Optional[types.TracebackType],
    ) -> typing.Literal[False]:
        self.db.close()
        return False


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
