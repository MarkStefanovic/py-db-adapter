import typing

import pyodbc

from py_db_adapter.domain import (
    db_adapter,
    exceptions,
    logger as domain_logger,
    rows as domain_rows,
    sql_predicate,
    table as domain_table,
)

__all__ = ("Repository",)

logger = domain_logger.root_logger.getChild("Repository")


class Repository:
    """Intersection between adapter.DbAdapter and a domain.Table"""

    def __init__(
        self,
        *,
        db: db_adapter.DbAdapter,
        table: domain_table.Table,
        change_tracking_columns: typing.Optional[typing.Iterable[str]] = None,
        read_only: bool = False,
        batch_size: int = 1_000,
    ):
        self._db = db
        self._table = table
        self._change_tracking_columns = (
            set(change_tracking_columns) if change_tracking_columns else set()
        )
        self._read_only = read_only
        self._batch_size = batch_size

    def add(self, *, cur: pyodbc.Cursor, rows: domain_rows.Rows) -> None:
        if self._read_only:
            raise exceptions.DatabaseIsReadOnly()
        else:
            self._db.add_rows(
                cur=cur,
                schema_name=self._table.schema_name,
                table_name=self._table.table_name,
                rows=rows,
                batch_size=self._batch_size,
            )

    def all(
        self, *, cur: pyodbc.Cursor, columns: typing.Optional[typing.Set[str]] = None
    ) -> domain_rows.Rows:
        return self._db.select_all(cur=cur, table=self._table, columns=columns)

    def create(self, *, cur: pyodbc.Cursor) -> bool:
        if self._read_only:
            raise exceptions.DatabaseIsReadOnly()
        else:
            return self._db.create_table(cur=cur, table=self._table)

    def delete(self, *, cur: pyodbc.Cursor, rows: domain_rows.Rows) -> None:
        if self._read_only:
            raise exceptions.DatabaseIsReadOnly()
        else:
            self._db.delete_rows(
                cur=cur,
                table=self._table,
                rows=rows,
                batch_size=self._batch_size,
            )

    def drop(self, *, cur: pyodbc.Cursor) -> bool:
        if self._read_only:
            raise exceptions.DatabaseIsReadOnly()
        else:
            return self._db.drop_table(
                cur=cur,
                table_name=self._table.table_name,
                schema_name=self._table.schema_name,
            )

    def fetch_rows_by_primary_key_values(
        self,
        *,
        cur: pyodbc.Cursor,
        rows: domain_rows.Rows,
        cols: typing.Optional[typing.Set[str]] = None,
    ) -> domain_rows.Rows:
        return self._db.fetch_rows_by_primary_key(
            cur=cur,
            table=self._table,
            rows=rows,
            cols=cols,
            batch_size=self._batch_size,
        )

    def keys(
        self,
        *,
        cur: pyodbc.Cursor,
        additional_cols: typing.Optional[typing.Set[str]] = None,
    ) -> domain_rows.Rows:
        return self._db.table_keys(
            cur=cur, table=self._table, additional_cols=additional_cols
        )

    def row_count(self, *, cur: pyodbc.Cursor) -> int:
        """Get the number of rows in a table"""
        return self._db.row_count(
            cur=cur,
            table_name=self._table.table_name,
            schema_name=self._table.schema_name,
        )

    def truncate(self, *, cur: pyodbc.Cursor) -> None:
        if self._read_only:
            raise exceptions.DatabaseIsReadOnly()
        else:
            return self._db.truncate_table(
                cur=cur,
                schema_name=self._table.schema_name,
                table_name=self._table.table_name,
            )

    def update(self, *, cur: pyodbc.Cursor, rows: domain_rows.Rows) -> None:
        if self._read_only:
            raise exceptions.DatabaseIsReadOnly()
        else:
            self._db.update_table(
                cur=cur, table=self._table, rows=rows, batch_size=self._batch_size
            )

    def where(
        self, *, cur: pyodbc.Cursor, predicate: sql_predicate.SqlPredicate
    ) -> domain_rows.Rows:
        return self._db.select_where(cur=cur, table=self._table, predicate=predicate)
