from __future__ import annotations

import pathlib
import pickle
import types
import typing

import pydantic

from py_db_adapter import domain, adapter

__all__ = (
    "Datasource",
    "postgres_pyodbc_datasource",
    "read_only_hive_datasource",
    "sql_server_pyodbc_datasource",
)

logger = domain.logger.getChild("Datasource")


class Datasource(pydantic.BaseModel):
    cache_dir: typing.Optional[pathlib.Path]
    compare_cols: typing.Optional[typing.Set[str]]
    db: domain.DbAdapter
    max_batch_size: int
    pk_cols: typing.Optional[typing.Set[str]]
    include_cols: typing.Optional[typing.Set[str]]
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
    ) -> typing.Dict[str, int]:
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

        if self.cache_dir:
            fp = self.cache_dir / f"{dest_table.table_name}.dest-keys.p"
            if fp.exists():
                dest_rows = pickle.load(open(file=fp, mode="rb"))
            else:
                dest_rows = dest_repo.keys(self.compare_cols)
        else:
            dest_rows = dest_repo.keys(self.compare_cols)

        if dest_rows.is_empty:
            logger.info(
                f"{self.table_name} is empty so the source rows will be fully loaded."
            )
            src_rows = src_repo.all(self.include_cols)
            dest_repo.add(src_rows)
            if self.cache_dir:
                dest_keys = src_rows.subset(
                    (src.pk_cols or set()) | (self.compare_cols or set())
                )
                dump_dest_keys(
                    cache_dir=self.cache_dir,
                    table_name=self.table_name,
                    dest_keys=dest_keys,
                )
            return {
                "added": src_rows.row_count,
                "deleted": 0,
                "updated": 0,
            }
        else:
            src_keys = src_repo.keys(self.compare_cols)
            if self.cache_dir:
                dump_dest_keys(
                    cache_dir=self.cache_dir,
                    table_name=self.table_name,
                    dest_keys=src_keys,
                )
            changes = dest_rows.compare(
                rows=src_keys,
                key_cols=pk_cols,
                compare_cols=compare_cols,
                ignore_missing_key_cols=True,
                ignore_extra_key_cols=True,
            )

            if (
                changes.rows_added.is_empty
                and changes.rows_deleted.is_empty
                and changes.rows_updated.is_empty
            ):
                logger.info(
                    "Source and destination matched already, so there was no need to refresh."
                )
                return {
                    "added": 0,
                    "deleted": 0,
                    "updated": 0,
                }
            else:
                if (rows_added := changes.rows_added.row_count) and add:
                    new_rows = src_repo.fetch_rows_by_primary_key_values(
                        rows=changes.rows_added, cols=src._table.column_names
                    )
                    dest_repo.add(new_rows)
                    logger.info(f"Added {rows_added} rows to [{self.table_name}].")
                if (rows_deleted := changes.rows_deleted.row_count) and delete:
                    dest_repo.delete(changes.rows_deleted)
                    logger.info(f"Deleted {rows_deleted} rows to [{self.table_name}].")
                if (rows_updated := changes.rows_updated.row_count) and update:
                    updated_rows = src_repo.fetch_rows_by_primary_key_values(
                        rows=changes.rows_updated, cols=src._table.column_names
                    )
                    dest_repo.update(updated_rows)
                    logger.info(f"Updated {rows_updated} rows to [{self.table_name}].")
                self.db.commit()
                return {
                    "added": rows_added,
                    "deleted": rows_deleted,
                    "updated": rows_updated,
                }

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
            include_cols=self.include_cols,
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
    include_cols: typing.Optional[typing.Set[str]] = None,
    max_batch_size: int = 1_000,
    read_only: bool = False,
    autocommit: bool = False,
) -> Datasource:
    sql_adapter = adapter.PostgreSQLAdapter()
    con = adapter.PyodbcConnection(
        db_name=db_name, fast_executemany=False, uri=db_uri, autocommit=autocommit
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
        include_cols=include_cols,
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
    include_cols: typing.Optional[typing.Set[str]] = None,
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
        include_cols=include_cols,
    )


def sql_server_pyodbc_datasource(
    *,
    db_name: str,
    db_uri: str,
    table_name: str,
    schema_name: typing.Optional[str] = None,
    cache_dir: typing.Optional[pathlib.Path] = None,
    compare_cols: typing.Optional[typing.Set[str]] = None,
    custom_pk_cols: typing.Optional[typing.Set[str]] = None,
    include_cols: typing.Optional[typing.Set[str]] = None,
    max_batch_size: int = 1_000,
    read_only: bool = False,
    fast_executemany: bool = True,
    autocommit: bool = False,
) -> Datasource:
    sql_adapter = adapter.SqlServerSQLAdapter()
    con = adapter.PyodbcConnection(
        db_name=db_name,
        fast_executemany=fast_executemany,
        uri=db_uri,
        autocommit=autocommit,
    )
    db = adapter.SqlServerPyodbcDbAdapter(con=con, sql_server_sql_adapter=sql_adapter)
    return Datasource(
        db=db,
        schema_name=schema_name,
        table_name=table_name,
        cache_dir=cache_dir,
        read_only=read_only,
        pk_cols=custom_pk_cols,
        compare_cols=compare_cols,
        include_cols=include_cols,
        max_batch_size=max_batch_size,
    )


def dump_dest_keys(
    cache_dir: pathlib.Path, table_name: str, dest_keys: domain.Rows
) -> None:
    fp = cache_dir / f"{table_name}.dest-keys.p"
    pickle.dump(dest_keys, open(fp, "wb"))
