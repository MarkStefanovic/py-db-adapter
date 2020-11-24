import pathlib
import typing

from py_db_adapter import adapter
from py_db_adapter.service.db_services import pyodbc_db_service

__all__ = ("PostgresPyodbcDbService",)


class PostgresPyodbcDbService(pyodbc_db_service.PyodbcDbService):
    def __init__(
        self,
        *,
        db_name: str,
        pyodbc_uri: str,
        cache_dir: typing.Optional[pathlib.Path] = None,
    ):
        super().__init__(
            db_name=db_name,
            pyodbc_uri=pyodbc_uri,
            cache_dir=cache_dir,
        )
        self._db_name = db_name
        self._pyodbc_uri = pyodbc_uri
        self._cache_dir = cache_dir
        self._sql_adapter = adapter.PostgreSQLAdapter()

    @property
    def db(self) -> adapter.DbAdapter:
        return adapter.PostgresPyodbcDbAdapter(
            con=self.con, postgres_sql_adapter=self._sql_adapter
        )
