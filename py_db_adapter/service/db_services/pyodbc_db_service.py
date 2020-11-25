import abc
import functools
import logging
import pathlib
import typing

from py_db_adapter import domain, adapter
from py_db_adapter.service import db_service

__all__ = ("PyodbcDbService",)

logger = logging.getLogger(__name__)


class PyodbcDbService(db_service.DbService, abc.ABC):
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

    @property
    @abc.abstractmethod
    def db(self) -> adapter.DbAdapter:
        raise NotImplementedError

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
