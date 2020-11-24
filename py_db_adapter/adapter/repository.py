from __future__ import annotations

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
        self._change_tracking_columns = (
            set(change_tracking_columns) if change_tracking_columns else set()
        )
        self._read_only = read_only
        self._batch_size = batch_size

    def add(self, /, rows: domain.Rows) -> None:
        if self._read_only:
            raise exceptions.DatabaseIsReadOnly()

        for batch in rows.batches(self._batch_size):
            sql = self._db.sql_adapter.add_rows(
                schema_name=self._table.schema_name,
                table_name=self._table.table_name,
                parameter_placeholder=self._db.connection.parameter_placeholder,
                rows=batch,
            )
            params = batch.as_dicts()
            self._db.connection.execute(sql, params=params, returns_rows=False)

    def all(self, /, columns: typing.Optional[typing.Set[str]] = None) -> domain.Rows:
        sql = self._db.sql_adapter.select_all(
            schema_name=self.table.schema_name,
            table_name=self.table.table_name,
            columns=columns,
        )
        result = self._db.connection.execute(sql, returns_rows=True)
        if result is None:
            return domain.Rows(
                column_names=columns or sorted(self._table.column_names),
                rows=[],
            )
        else:
            return result

    def create(self) -> None:
        if self._read_only:
            raise exceptions.DatabaseIsReadOnly()

        self._db.connection.execute(
            self._db.sql_adapter.definition(self._table), returns_rows=False
        )

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
            self._db.connection.execute(sql, params, returns_rows=False)

    def drop(self) -> None:
        if self._read_only:
            raise exceptions.DatabaseIsReadOnly()

        sql = self._db.sql_adapter.drop(
            schema_name=self._table.schema_name,
            table_name=self._table.table_name,
        )
        self._db.connection.execute(sql, returns_rows=False)

    @property
    def full_table_name(self) -> str:
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
            if len(self._pk_cols) == 1:  # where-clause uses IN (...)
                row_batch = self._db.connection.execute(sql, returns_rows=True)
            else:
                pks = batch.subset(self._table.primary_key_column_names).as_dicts()
                row_batch = self._db.connection.execute(
                    sql, params=pks, returns_rows=True
                )
            # params = batch.as_dicts()
            # row_batch = self._db.connection.execute(sql, params=params)
            if row_batch:
                batches.append(row_batch)
        return domain.Rows.concat(batches)

    @property
    def _pk_cols(self) -> typing.Set[domain.Column]:
        return {col for col in self._table.columns if col.primary_key}

    def keys(self, /, include_change_tracking_cols: bool = True) -> domain.Rows:
        if include_change_tracking_cols:
            sql = self._db.sql_adapter.select_keys(
                schema_name=self._table.schema_name,
                table_name=self._table.table_name,
                pk_cols=self._table.primary_key_column_names,
                change_tracking_cols=set(self._change_tracking_columns),
                include_change_tracking_cols=include_change_tracking_cols,
            )
        else:
            sql = self._db.sql_adapter.select_keys(
                schema_name=self._table.schema_name,
                table_name=self._table.table_name,
                pk_cols=self._table.primary_key_column_names,
                change_tracking_cols=set(),
                include_change_tracking_cols=include_change_tracking_cols,
            )
        result = self._db.connection.execute(sql, returns_rows=True)
        if result is None:
            return domain.Rows(
                column_names=sorted(
                    self._table.primary_key_column_names
                    | set(self._change_tracking_columns)
                ),
                rows=[],
            )
        else:
            return result

    def row_count(self) -> typing.Optional[int]:
        """Get the number of rows in a table"""
        result = self._db.connection.execute(
            self._db.sql_adapter.row_count(
                schema_name=self._table.schema_name,
                table_name=self._table.table_name,
            ),
            returns_rows=True,
        )
        if result:
            return result.first_value()
        else:
            return None

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
        self._db.connection.execute(sql, returns_rows=False)

    def update(self, /, rows: domain.Rows) -> None:
        if self._read_only:
            raise exceptions.DatabaseIsReadOnly()

        for batch in rows.batches(self._batch_size):
            sql = self._db.sql_adapter.update(
                parameter_placeholder=self._db.connection.parameter_placeholder,
                pk_cols=self._table.primary_key_column_names,
                column_names=set(batch.column_names),
                schema_name=self._table.schema_name,
                table_name=self._table.table_name,
            )
            pk_cols = sorted(self._table.primary_key_column_names)
            non_pk_cols = sorted(
                col
                for col in self._table.column_names
                if col not in self._table.primary_key_column_names
            )
            unordered_params = batch.as_dicts()
            param_order = non_pk_cols + pk_cols
            ordered_params = [
                {k: row[k] for k in param_order} for row in unordered_params
            ]
            self._db.connection.execute(sql, params=ordered_params, returns_rows=False)
