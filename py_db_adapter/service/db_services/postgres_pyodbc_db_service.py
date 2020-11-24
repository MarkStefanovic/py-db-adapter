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

    @functools.cached_property
    def _con(self) -> adapter.PyodbcConnection:
        return adapter.PyodbcConnection(
            db_name=self._db_name, fast_executemany=False, uri=self._pyodbc_uri
        )

    @functools.cached_property
    def _db(self) -> adapter.PostgresPyodbcDbAdapter:
        sql_adapter = adapter.PostgreSQLAdapter()

        return adapter.PostgresPyodbcDbAdapter(con=self._con, postgres_sql_adapter=sql_adapter)

    @functools.lru_cache
    def _create_repo(
        self,
        *,
        schema_name: typing.Optional[str],
        table_name: str,
        change_tracking_columns: typing.Set[str],
        custom_pk_columns: typing.Optional[str],
    ) -> adapter.Repository:
        table = self._con.inspect_table(
            table_name=table_name,
            schema_name=schema_name,
            custom_pk_cols=custom_pk_columns,
            cache_dir=self._cache_dir,
        )
        return adapter.Repository(
            db=self._db, table=table, change_tracking_columns=change_tracking_columns
        )

    def row_count(self, *, schema_name: typing.Optional[str], table_name: str) -> int:
        try:
            return self._con.fast_row_count(
                table_name=self.table_name, schema_name=self.schema_name
            )
        except NotImplementedError:
            return self._repo.row_count()

    def inspect_table(
        self, *, schema_name: typing.Optional[str], table_name: str
    ) -> domain.Table:
        return self._repo.connection.inspect_table(
            table_name=self.table_name,
            schema_name=self.schema_name,
            cache_dir=self._cache_dir,
        )

    def exists(self, *, schema_name: typing.Optional[str], table_name: str) -> bool:
        return self._db.table_exists(
            table_name=self.table_name, schema_name=self.schema_name
        )

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
        pass
