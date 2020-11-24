from __future__ import annotations

import itertools
import logging
import typing

from py_db_adapter import domain
from py_db_adapter.adapter import db_adapter

__all__ = ("Repository",)

from py_db_adapter.domain import exceptions

logger = logging.getLogger(__name__)


class Repository:
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
        self._change_tracking_columns = set(change_tracking_columns) if change_tracking_columns else set()
        self._read_only = read_only
        self._batch_size = batch_size

    def add(self, /, rows: domain.Rows) -> None:
        if self._read_only:
            raise exceptions.DatabaseIsReadOnly()

        for batch in rows.batches(self._batch_size):
            sql = self._db.sql_adapter.add_rows(
                parameter_placeholder=self._db.connection.parameter_placeholder,
                rows=batch,
            )
            params = batch.as_dicts()
            self._db.connection.execute(sql, params=params)

    def all(self, /, columns: typing.Optional[typing.Set[str]] = None) -> domain.Rows:
        sql = self._db.sql_adapter.select_all(
            schema_name=self.table.schema_name,
            table_name=self.table.table_name,
            columns=columns,
        )
        return self._db.connection.execute(sql)

    def create(self) -> None:
        if self._read_only:
            raise exceptions.DatabaseIsReadOnly()

        return self._db.connection.execute(self._db.sql_adapter.definition(self._table))

    def delete(self, /, rows: domain.Rows) -> None:
        if self._read_only:
            raise exceptions.DatabaseIsReadOnly()

        sql = self._db.sql_adapter.delete(
            schema_name=self._table.schema_name,
            table_name=self._table.table_name,
            pk_cols=self._table.primary_key_column_names,
            parameter_placeholder=self._db.connection.parameter_placeholder,
            row_cols=rows.column_names,
        )
        for batch in rows.batches(self._batch_size):
            params = batch.as_dicts()
            self._db.connection.execute(sql, params)

    def drop(self) -> None:
        if self._read_only:
            raise exceptions.DatabaseIsReadOnly()

        sql = self._db.sql_adapter.drop(
            schema_name=self._table.schema_name,
            table_name=self._table.table_name,
        )
        self._db.connection.execute(sql)

    @property
    def full_table_name(self):
        return self._db.sql_adapter.full_table_name(
            schema_name=self._table.schema_name, table_name=self._table.table_name
        )

    def fetch_rows_by_primary_key_values(
        self,
        *,
        rows: domain.Rows,
        cols: typing.Optional[typing.Set[str]] = None,
    ) -> domain.Rows:
        batches: typing.List[domain.Rows] = []
        for batch in rows.batches(self._batch_size):
            sql = self._db.sql_adapter.fetch_rows_by_primary_key_values(
                schema_name=self._table.schema_name,
                table_name=self._table.table_name,
                rows=batch,
                pk_cols=self._pk_cols,
                select_cols=cols,
            )
            params = batch.as_dicts()
            row_batch = self._db.connection.execute(sql, params=params)
            batches.append(row_batch)
        return domain.Rows.concat(batches)

    @property
    def _pk_cols(self) -> typing.Set[domain.Column]:
        return {col for col in self._table.columns if col.primary_key}

    # def keys(self, /, include_change_tracking_cols: bool = True) -> domain.Rows:
    #     if include_change_tracking_cols:
    #         sql = self._db.sql_adapter.select_keys(
    #             pk_cols=self._table.primary_key_column_names,
    #             change_tracking_cols=set(self._change_tracking_columns),
    #             include_change_tracking_cols=include_change_tracking_cols,
    #         )
    #     else:
    #         sql = self._db.sql_adapter.select_keys(
    #             pk_cols=self._table.primary_key_column_names,
    #             change_tracking_cols=set(),
    #             include_change_tracking_cols=include_change_tracking_cols,
    #         )
    #     return self._db.connection.execute(sql)

    def row_count(self) -> int:
        """Get the number of rows in a table"""
        return self._db.connection.execute(
            self._db.sql_adapter.row_count(
                schema_name=self._table.schema_name,
                table_name=self._table.table_name,
            )
        ).first_value()

    @property
    def table(self) -> domain.Table:
        return self._table

    def truncate(self) -> None:
        if self._read_only:
            raise exceptions.DatabaseIsReadOnly()

        sql = self._db.sql_adapter.truncate(
            schema_name=self._table.schema_name,
            table_name=self._table.table_name,
        )
        self._db.connection.execute(sql)

    def update(self, /, rows: domain.Rows) -> None:
        if self._read_only:
            raise exceptions.DatabaseIsReadOnly()

        for batch in rows.batches(self._batch_size):
            sql = self._db.sql_adapter.update(
                parameter_placeholder=self._db.connection.parameter_placeholder,
                pk_cols=self._pk_cols,
                rows=batch,
                schema_name=self._table.schema_name,
                table_name=self._table.table_name,
            )
            self._db.connection.execute(sql)

    def upsert_rows(
        self,
        *,
        rows: domain.Rows,
        add: bool = True,
        update: bool = True,
        delete: bool = True,
        ignore_missing_key_cols: bool = True,
        ignore_extra_key_cols: bool = True,
    ) -> None:
        if self._read_only:
            raise exceptions.DatabaseIsReadOnly()

        pk_col_names = {col.column_name for col in self._pk_cols}
        col_names = pk_col_names | self._change_tracking_columns
        dest_rows = self.all(col_names)
        if dest_rows.is_empty:
            logger.info(f"{self.full_table_name} is empty so the source rows will be fully loaded.")
            self.add(rows)
        else:
            changes = dest_rows.compare(
                rows=rows,
                key_cols=pk_col_names,
                compare_cols=self._change_tracking_columns,
                ignore_missing_key_cols=ignore_missing_key_cols,
                ignore_extra_key_cols=ignore_extra_key_cols,
            )
            common_cols = self.table.column_names & set(rows.column_names)
            if changes.rows_added.row_count and add:
                new_rows = self.fetch_rows_by_primary_key_values(
                    rows=changes.rows_added, cols=common_cols
                )
                self.add(new_rows)
            if changes.rows_deleted.row_count and delete:
                self.delete(changes.rows_deleted)
            if changes.rows_updated.row_count and update:
                updated_rows = self.fetch_rows_by_primary_key_values(
                    rows=changes.rows_updated, cols=common_cols
                )
                self.update(updated_rows)
