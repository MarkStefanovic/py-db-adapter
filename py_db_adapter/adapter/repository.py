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
        change_tracking_columns: typing.Optional[typing.Iterable[str]] = None,
        db: db_adapter.DbAdapter,
        table: domain.Table,
        read_only: bool = False,
    ):
        self._change_tracking_columns = change_tracking_columns
        self._db = db
        self._table = table
        self._read_only = read_only

    def add(self, /, rows: domain.Rows) -> None:
        if self._read_only:
            raise exceptions.DatabaseIsReadOnly()
        else:
            sql = self._db.sql_adapter.add_rows(
                parameter_placeholder=self._db.connection.parameter_placeholder,
                rows=rows,
            )
            params = rows.as_dicts()
            self._db.connection.execute(sql, params=params)

    def all(self, /, columns: typing.Optional[typing.Set[str]]) -> domain.Rows:
        sql = self._db.sql_adapter.select_all(
            schema_name=self.table.schema_name, table_name=self.table.table_name
        )
        return self._db.connection.execute(sql)

    @property
    def change_tracking_columns(self) -> typing.Set[str]:
        return set(self._change_tracking_columns)

    def create(self) -> None:
        if self._read_only:
            raise exceptions.DatabaseIsReadOnly()
        return self._db.connection.execute(self._db.sql_adapter.definition(self._table))

    def delete(self, /, rows: domain.Rows) -> None:
        if self._read_only:
            raise exceptions.DatabaseIsReadOnly()
        else:
            sql = self._db.sql_adapter.delete(
                schema_name=self._table.schema_name,
                table_name=self._table.table_name,
                pk_cols=self._table.primary_key_column_names,
                parameter_placeholder=self._db.connection.parameter_placeholder,
                row_cols=rows.column_names,
            )
            params = rows.as_dicts()
            self._db.connection.execute(sql, params)

    def drop(self) -> None:
        if self._read_only:
            raise exceptions.DatabaseIsReadOnly()
        else:
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
        pk_cols = {col for col in self._table.columns if col.primary_key}
        sql = self._db.sql_adapter.fetch_rows_by_primary_key_values(
            schema_name=self._table.schema_name,
            table_name=self._table.table_name,
            rows=rows,
            pk_cols=pk_cols,
            select_cols=cols,
        )
        params = rows.as_dicts()
        return self._db.connection.execute(sql, params=params)

    def keys(self, /, include_change_tracking_cols: bool = True) -> domain.Rows:
        if include_change_tracking_cols:
            sql = self._db.sql_adapter.select_keys(
                pk_cols=self._table.primary_key_column_names,
                change_tracking_cols=set(self._change_tracking_columns),
                include_change_tracking_cols=include_change_tracking_cols,
            )
        else:
            sql = self._db.sql_adapter.select_keys(
                pk_cols=self._table.primary_key_column_names,
                change_tracking_cols=set(),
                include_change_tracking_cols=include_change_tracking_cols,
            )
        return self._db.connection.execute(sql)

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

    def update(self, /, rows: domain.Rows) -> None:
        if self._read_only:
            raise exceptions.DatabaseIsReadOnly()
        else:
            pk_cols = {col for col in self._table.columns if col.primary_key}
            sql = self._db.sql_adapter.update(
                schema_name=self._table.schema_name,
                table_name=self._table.table_name,
                pk_cols=pk_cols,
                rows=rows,
            )
            return self._db.connection.execute(sql)

    def upsert_rows(
        self,
        *,
        rows: domain.Rows,
        add: bool = True,
        update: bool = True,
        delete: bool = True,
    ) -> None:
        # TODO
        pass
        # src_keys = rows.subset(
        #     key_columns=common_key_cols,
        #     value_columns: typing.Optional[typing.Set[str]] = None,
        # )
        # changes = domain.compare_rows(
        #     key_cols=self.table.primary_key_column_names,
        #     src_rows=src_keys,
        #     dest_rows=self.keys(True),
        # )
        # common_cols = self.table.column_names.intersection(
        #     rows.column_names
        # )
        # if changes["added"].row_count and add:
        #     new_rows = self.fetch_rows_by_primary_key_values(
        #         rows=changes["added"], columns=common_cols
        #     )
        #     self.add(new_rows)
        # if changes["deleted"].row_count and delete:
        #     self.delete(changes["deleted"])
        # if changes["updated"].row_count and update:
        #     updated_rows = self.fetch_rows_by_primary_key_values(
        #         rows=changes["updated"], columns=common_cols
        #     )
        #     self.update(updated_rows)
