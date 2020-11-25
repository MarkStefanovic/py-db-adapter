import functools
import pathlib
import typing

from py_db_adapter import domain, adapter
from py_db_adapter.service import db_service

__all__ = ("HivePyodbcDbService",)


class HivePyodbcDbService(db_service.DbService):
    def __init__(
        self,
        *,
        db_name: str,
        pyodbc_uri: str,
        cache_dir: typing.Optional[pathlib.Path] = None,
    ) -> None:
        self._db_name = db_name
        self._pyodbc_uri = pyodbc_uri
        self._cache_dir = cache_dir

    @property
    def cache_dir(self) -> typing.Optional[pathlib.Path]:
        return self._cache_dir

    @functools.cached_property
    def con(self) -> adapter.PyodbcConnection:  # type: ignore
        return adapter.HivePyodbcConnection(db_name=self._db_name, uri=self._pyodbc_uri)

    @functools.cached_property
    def db(self) -> adapter.HivePyodbcDbAdapter:  # type: ignore
        sql_adapter = adapter.HiveSQLAdapter()
        return adapter.HivePyodbcDbAdapter(con=self.con, hive_sql_adapter=sql_adapter)

    def fast_row_count(
        self, *, schema_name: typing.Optional[str], table_name: str
    ) -> int:
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
        src_db: db_service.DbService,
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
        raise domain.exceptions.DatabaseIsReadOnly()
