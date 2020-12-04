from __future__ import annotations

import pathlib
import typing

import pyodbc

from py_db_adapter import domain
from py_db_adapter.adapter import db_connection, pyodbc_inspector
from py_db_adapter.domain import exceptions

__all__ = ("PyodbcConnection",)

logger = domain.logger.getChild("PyodbcConnection")


class PyodbcConnection(db_connection.DbConnection):
    def __init__(
        self, *, db_name: str, fast_executemany: bool, uri: str, autocommit: bool
    ):
        super().__init__()

        self._db_name = db_name
        self._fast_executemany = fast_executemany
        self._uri = uri
        self._autocommit = autocommit

        self._con: typing.Optional[pyodbc.Connection] = None
        self._cur: typing.Optional[pyodbc.Cursor] = None

    def close(self) -> None:
        if self._cur is not None:
            self._cur.close()
            self._cur = None
            logger.debug("Closed cursor.")
        if self._con is not None:
            self._con.close()
            self._con = None
            logger.debug(f"Closed connection to {self._db_name}.")

    def execute(
        self,
        *,
        sql: str,
        params: typing.Optional[typing.List[typing.Dict[str, typing.Any]]] = None,
    ) -> None:
        std_sql = domain.standardize_sql(sql)
        if self._con is None:
            raise exceptions.DeveloperError(
                "Attempted to run .execute() outside of a with block."
            )
        logger.debug(f"EXECUTE:\n\t{std_sql}\n\tparams={params}")
        print(f"EXECUTE:\n\t{std_sql}\n\tparams={params}")  # TODO: remove print statements
        positional_params = [tuple(param.values()) for param in params or {}]
        if self._cur is None:
            self._cur = self._con.cursor()
            self._cur.fast_executemany = self._fast_executemany
            logger.debug("Opened cursor.")

        if params is None:
            self._cur.execute(std_sql)
        elif len(params) > 1:
            self._cur.executemany(std_sql, positional_params)
        else:
            self._cur.execute(std_sql, positional_params[0])

    def fetch(
        self,
        *,
        sql: str,
        params: typing.Optional[typing.List[typing.Dict[str, typing.Any]]] = None,
    ) -> domain.Rows:
        std_sql = domain.standardize_sql(sql)
        if self._con is None:
            raise exceptions.DeveloperError(
                "Attempted to run .execute() outside of a with block."
            )

        logger.debug(f"FETCH:\n\t{std_sql}\n\tparams={params}")
        print(f"FETCH:\n\t{std_sql}\n\tparams={params}")  # TODO: remove print statements
        positional_params = [tuple(param.values()) for param in params or {}]
        if self._cur is None:
            self._cur = self._con.cursor()
            self._cur.fast_executemany = self._fast_executemany
            logger.debug("Opened cursor.")

        if params is None:
            result = self._cur.execute(std_sql)
        elif len(params) > 1:
            result = self._cur.executemany(std_sql, positional_params)
        else:
            result = self._cur.execute(std_sql, positional_params[0])

        if rows := result.fetchall():
            column_names = [description[0] for description in self._cur.description]
            return domain.Rows(
                column_names=column_names, rows=[tuple(row) for row in rows]
            )
        else:
            return domain.Rows(column_names=[], rows=[])

    def commit(self) -> None:
        if self._con is None:
            raise exceptions.DeveloperError(
                "Attempted to run .execute() outside of a with block."
            )
        else:
            self._con.commit()

    @property
    def handle(self) -> pyodbc.Connection:
        if self._con is None:
            self._con = pyodbc.connect(self._uri, autocommit=self._autocommit)
        return self._con

    def inspect_table(
        self,
        *,
        table_name: str,
        schema_name: typing.Optional[str] = None,
        pk_cols: typing.Optional[typing.Set[str]] = None,
        cache_dir: typing.Optional[pathlib.Path] = None,
    ) -> domain.Table:
        if cache_dir is None:
            return pyodbc_inspector.pyodbc_inspect_table(
                con=self.handle,
                table_name=table_name,
                schema_name=schema_name,
                custom_pk_cols=pk_cols,
            )
        else:
            return pyodbc_inspector.pyodbc_inspect_table_and_cache(
                con=self.handle,
                table_name=table_name,
                schema_name=schema_name,
                custom_pk_cols=pk_cols,
                cache_dir=cache_dir,
            )

    def open(self) -> None:
        if self._con is None:  # noqa
            self._con = pyodbc.connect(self._uri, autocommit=self._autocommit)
            logger.debug(f"Opened connection to {self._db_name}.")

    def parameter_placeholder(self, /, column_name: str) -> str:
        return "?"

    def rollback(self) -> None:
        if self._con is None:
            raise exceptions.DeveloperError(
                "Attempted to run .execute() outside of a with block."
            )
        else:
            self._con.rollback()
