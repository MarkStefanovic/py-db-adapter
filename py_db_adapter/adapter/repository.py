from __future__ import annotations

import typing

from py_db_adapter import domain
from py_db_adapter.adapter import db_adapter
from py_db_adapter.domain import exceptions

__all__ = ("Repository",)

logger = domain.logger.getChild(__name__)


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

        for batch in rows.batches(self._batch_size):
            sql = self._db._sql_adapter.add_rows(
                schema_name=self._table.schema_name,
                table_name=self._table.table_name,
                parameter_placeholder=self._db._connection.parameter_placeholder,
                rows=batch,
            )
            params = batch.as_dicts()
            self._db.execute(sql=sql, params=params)

    def all(self, /, columns: typing.Optional[typing.Set[str]] = None) -> domain.Rows:
        sql = self._db._sql_adapter.select_all(
            schema_name=self.table.schema_name,
            table_name=self.table.table_name,
            columns=columns,
        )
        result = self._db.fetch(sql=sql)
        if result is None:
            return domain.Rows(
                column_names=columns or sorted(self._table.column_names),
                rows=[],
            )
        else:
            return result

    def create(self) -> bool:
        if self._read_only:
            raise exceptions.DatabaseIsReadOnly()
        else:
            return self._db.create_table(self._table)

    def delete(self, /, rows: domain.Rows) -> None:
        if self._read_only:
            raise exceptions.DatabaseIsReadOnly()

        sql = self._db._sql_adapter.delete(
            schema_name=self._table.schema_name,
            table_name=self._table.table_name,
            pk_cols=self._table.pk_cols,
            parameter_placeholder=self._db._connection.parameter_placeholder,
            row_cols=rows.column_names,
        )
        for batch in rows.batches(self._batch_size):
            params = batch.as_dicts()
            self._db.execute(sql=sql, params=params)

    def drop(self) -> bool:
        if self._read_only:
            raise exceptions.DatabaseIsReadOnly()

        return self._db.drop_table(
            table_name=self._table.table_name,
            schema_name=self._table.schema_name,
        )

    @property
    def full_table_name(self) -> str:
        return self._db._sql_adapter.full_table_name(
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
            sql = self._db._sql_adapter.fetch_rows_by_primary_key_values(
                schema_name=self._table.schema_name,
                table_name=self._table.table_name,
                rows=batch,
                pk_cols=self._pk_cols,
                select_cols=cols,
            )
            if len(self._pk_cols) == 1:  # where-clause uses IN (...)
                row_batch = self._db.fetch(sql=sql)
            else:
                pks = batch.subset(self._table.pk_cols).as_dicts()
                row_batch = self._db.fetch(sql=sql, params=pks)
            if row_batch:
                batches.append(row_batch)
        return domain.Rows.concat(batches)

    @property
    def _pk_cols(self) -> typing.Set[domain.Column]:
        return {
            col for col in self._table.columns if col.column_name in self._table.pk_cols
        }

    def keys(self, /, include_change_tracking_cols: bool = True) -> domain.Rows:
        if include_change_tracking_cols:
            sql = self._db._sql_adapter.select_keys(
                schema_name=self._table.schema_name,
                table_name=self._table.table_name,
                pk_cols=self._table.pk_cols,
                change_tracking_cols=set(self._change_tracking_columns),
                include_change_tracking_cols=include_change_tracking_cols,
            )
        else:
            sql = self._db._sql_adapter.select_keys(
                schema_name=self._table.schema_name,
                table_name=self._table.table_name,
                pk_cols=self._table.pk_cols,
                change_tracking_cols=set(),
                include_change_tracking_cols=include_change_tracking_cols,
            )
        result = self._db.fetch(sql=sql)
        if result is None:
            return domain.Rows(
                column_names=sorted(
                    self._table.pk_cols | set(self._change_tracking_columns)
                ),
                rows=[],
            )
        else:
            return result

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

        sql = self._db._sql_adapter.truncate(
            schema_name=self._table.schema_name,
            table_name=self._table.table_name,
        )
        self._db.execute(sql=sql)

    def update(self, /, rows: domain.Rows) -> None:
        if self._read_only:
            raise exceptions.DatabaseIsReadOnly()

        for batch in rows.batches(self._batch_size):
            sql = self._db._sql_adapter.update(
                parameter_placeholder=self._db._connection.parameter_placeholder,
                pk_cols=self._table.pk_cols,
                column_names=set(batch.column_names),
                schema_name=self._table.schema_name,
                table_name=self._table.table_name,
            )
            pk_cols = sorted(self._table.pk_cols)
            non_pk_cols = sorted(
                col
                for col in self._table.column_names
                if col not in self._table.pk_cols
            )
            unordered_params = batch.as_dicts()
            param_order = non_pk_cols + pk_cols
            ordered_params = [
                {k: row[k] for k in param_order} for row in unordered_params
            ]
            self._db.execute(sql=sql, params=ordered_params)
