import functools
import pathlib
import typing

from py_db_adapter import domain, adapter
from py_db_adapter.service import db_service, DbService

__all__ = ("PostgresPyodbcDbService",)


class PostgresPyodbcDbService(db_service.DbService):
    def __init__(
        self,
        *,
        db_name: str,
        pyodbc_uri: str,
        cache_dir: typing.Optional[pathlib.Path] = None,
    ):
        self._db_name = db_name
        self._pyodbc_uri = pyodbc_uri
        self._cache_dir = cache_dir

    @property
    def cache_dir(self) -> typing.Optional[pathlib.Path]:
        return self._cache_dir

    @functools.cached_property
    def con(self) -> adapter.PyodbcConnection:  # type: ignore
        return adapter.PyodbcConnection(
            db_name=self._db_name, fast_executemany=False, uri=self._pyodbc_uri
        )

    @functools.cached_property
    def db(self) -> adapter.PostgresPyodbcDbAdapter:  # type: ignore
        sql_adapter = adapter.PostgreSQLAdapter()
        return adapter.PostgresPyodbcDbAdapter(
            con=self.con, postgres_sql_adapter=sql_adapter
        )

    def fast_row_count(
        self, *, schema_name: typing.Optional[str], table_name: str
    ) -> typing.Optional[int]:
        return self.db.fast_row_count(table_name=table_name, schema_name=schema_name)

    def inspect_table(
        self, *, schema_name: typing.Optional[str], table_name: str
    ) -> domain.Table:
        return self.con.inspect_table(
            table_name=table_name,
            schema_name=schema_name,
            cache_dir=self._cache_dir,
        )

    def table_exists(
        self, *, schema_name: typing.Optional[str], table_name: str
    ) -> bool:
        return self.db.table_exists(table_name=table_name, schema_name=schema_name)

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
    ) -> None:
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
        )
        dest_repo = self.create_repo(
            schema_name=dest_schema_name,
            table_name=dest_table_name,
            change_tracking_columns=compare_cols,
            pk_columns=pk_cols,
        )
        dest_repo.upsert_rows(
            rows=src_repo.keys(True),
            add=add,
            update=update,
            delete=delete,
            ignore_missing_key_cols=True,
            ignore_extra_key_cols=True,
        )
