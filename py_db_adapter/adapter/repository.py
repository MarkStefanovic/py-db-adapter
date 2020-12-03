from __future__ import annotations

import typing

from py_db_adapter import domain
from py_db_adapter.adapter import db_adapter
from py_db_adapter.domain import exceptions

__all__ = ("Repository",)

logger = domain.logger.getChild("Repository")


class Repository:
    """Intersection between adapter.DbAdapter and a domain.Table"""

    def __init__(
        self,
        *,
        db: db_adapter.DbAdapter,
        table: domain.Table,
        change_tracking_columns: typing.Optional[typing.Iterable[str]] = None,
        read_only: bool = False,
        batch_size: int = 10_000,
    ):
        self._db = db
        self._table = table
        self._change_tracking_columns = (
            set(change_tracking_columns) if change_tracking_columns else set()
        )
        self._read_only = read_only
        self._batch_size = batch_size

    def add(self, /, rows: domain.Rows) -> None:
        if self._read_only:
            raise exceptions.DatabaseIsReadOnly()
        else:
            self._db.add_rows(
                schema_name=self._table.schema_name,
                table_name=self._table.table_name,
                rows=rows,
                batch_size=self._batch_size,
            )

    def all(self, /, columns: typing.Optional[typing.Set[str]] = None) -> domain.Rows:
        return self._db.select_all(table=self._table, columns=columns)

    def create(self) -> bool:
        if self._read_only:
            raise exceptions.DatabaseIsReadOnly()
        else:
            return self._db.create_table(self._table)

    def delete(self, /, rows: domain.Rows) -> None:
        if self._read_only:
            raise exceptions.DatabaseIsReadOnly()
        else:
            self._db.delete_rows(
                table=self._table,
                rows=rows,
                batch_size=self._batch_size,
            )

    def drop(self) -> bool:
        if self._read_only:
            raise exceptions.DatabaseIsReadOnly()
        else:
            return self._db.drop_table(
                table_name=self._table.table_name,
                schema_name=self._table.schema_name,
            )

    def fetch_rows_by_primary_key_values(
        self,
        *,
        rows: domain.Rows,
        cols: typing.Optional[typing.Set[str]] = None,
    ) -> domain.Rows:
        return self._db.fetch_rows_by_primary_key(
            table=self._table,
            rows=rows,
            cols=cols,
            batch_size=self._batch_size,
        )

    def keys(self, /, include_change_tracking_cols: bool = True) -> domain.Rows:
        if include_change_tracking_cols:
            return self._db.table_keys(
                table=self._table, compare_cols=self._change_tracking_columns
            )
        else:
            return self._db.table_keys(table=self._table, compare_cols=None)

    def row_count(self) -> int:
        """Get the number of rows in a table"""
        return self._db.row_count(
            table_name=self._table.table_name, schema_name=self._table.schema_name
        )

    @property
    def table(self) -> domain.Table:
        return self._table

    def truncate(self) -> None:
        if self._read_only:
            raise exceptions.DatabaseIsReadOnly()
        else:
            return self._db.truncate_table(
                schema_name=self._table.schema_name, table_name=self._table.table_name
            )

    def update(self, /, rows: domain.Rows) -> None:
        if self._read_only:
            raise exceptions.DatabaseIsReadOnly()
        else:
            self._db.update_table(
                table=self._table, rows=rows, batch_size=self._batch_size
            )
