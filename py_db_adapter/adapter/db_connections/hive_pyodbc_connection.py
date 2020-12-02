from __future__ import annotations

import logging
import typing

import pyodbc

from py_db_adapter.adapter.db_connections import pyodbc_connection
from py_db_adapter.domain import exceptions

__all__ = ("HivePyodbcConnection",)

logger = logging.getLogger(__name__)


class HivePyodbcConnection(pyodbc_connection.PyodbcConnection):
    def __init__(self, *, db_name: str, uri: str):
        super().__init__(
            db_name=db_name, uri=uri, fast_executemany=False, autocommit=True
        )

        self._db_name = db_name
        self._uri = uri

        self._con: typing.Optional[pyodbc.Connection] = None
        self._cur: typing.Optional[pyodbc.Cursor] = None

    def commit(self) -> None:
        raise exceptions.DatabaseIsReadOnly()

    def rollback(self) -> None:
        raise exceptions.DatabaseIsReadOnly()
