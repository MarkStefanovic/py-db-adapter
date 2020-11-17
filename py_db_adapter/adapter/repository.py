from __future__ import annotations

import logging
import typing

from py_db_adapter import domain
from py_db_adapter.adapter import sql_adapter, db_connection

__all__ = ("Repository",)

from py_db_adapter.domain import exceptions

logger = logging.getLogger(__name__)


class Repository:
    def __init__(
        self,
        *,
        change_tracking_columns: typing.Optional[typing.Iterable[str]] = None,
        connection: db_connection.DbConnection,
        sql_adapter: sql_adapter.SqlAdapter,
        table: domain.Table,
        read_only: bool = False,
    ):
        self._change_tracking_columns = change_tracking_columns
        self._connection = connection
        self._sql_adapter = sql_adapter
        self._table = table
        self._read_only = read_only

    def add(self, /, rows: domain.Rows) -> None:
        if self._read_only:
            raise exceptions.DatabaseIsReadOnly()

        col_name_csv = ",".join(
            self._sql_adapter.wrap(col_name) for col_name in rows.column_names
        )
        dummy_csv = ",".join(
            self._connection.parameter_placeholder(col_name)
            for col_name in rows.column_names
        )
        sql = (
            f"INSERT INTO {self.full_table_name} ({col_name_csv}) "
            f"VALUES ({dummy_csv})"
        )
        params = rows.as_dicts()
        logger.debug(f"Executing SQL:\n\t{sql}\n\t{params=}")
        self._connection.execute(sql, params=params)

    def all(self) -> domain.Rows:
        return self._connection.execute(sql=self._sql_adapter.select_all(self._table))

    @property
    def change_tracking_columns(self) -> typing.Set[str]:
        return set(self._change_tracking_columns)

    def create(self) -> None:
        if self._read_only:
            raise exceptions.DatabaseIsReadOnly()
        return self._connection.execute(self._sql_adapter.definition(self._table))

    def delete(self, /, rows: domain.Rows) -> None:
        if self._read_only:
            raise exceptions.DatabaseIsReadOnly()

        where_clause = " AND ".join(
            f"{self._sql_adapter.wrap(col_name)} = {self._connection.parameter_placeholder(col_name)}"
            for col_name in rows.column_names
            if col_name in self._table.primary_key_column_names
        )
        sql = f"DELETE FROM {self.full_table_name} " f"WHERE {where_clause}"
        params = rows.as_dicts()
        logger.debug(f"Executing SQL:\n\t{sql}\n\t{params=}")
        self._connection.execute(sql, params)

    def drop(self, /, cascade: bool = False) -> None:
        if self._read_only:
            raise exceptions.DatabaseIsReadOnly()

        self._connection.execute(self._sql_adapter.drop(self._table, cascade=cascade))

    @property
    def full_table_name(self):
        return self._sql_adapter.full_table_name(
            schema_name=self._table.schema_name, table_name=self._table.table_name
        )

    def fetch_rows_by_primary_key_values(
        self, *, rows: domain.Rows, columns: typing.Optional[typing.Set[str]]
    ) -> domain.Rows:
        if len(self._table.primary_key_column_names) == 1:
            pk_col_name = list(self._table.primary_key_column_names)[0]
            wrapped_pk_col_name = self._sql_adapter.wrap(pk_col_name)
            col_adapter = next(
                col
                for col in self._sql_adapter.columns
                if col.column_metadata.column_name == pk_col_name
            )
            pk_values = rows.column(pk_col_name)
            pk_values_csv = ",".join(col_adapter.literal(v) for v in pk_values)
            where_clause = f"{wrapped_pk_col_name} IN ({pk_values_csv})"
        else:
            wrapped_pk_col_names = {}
            for col_name in rows.column_names:
                if col_name in self._table.primary_key_column_names:
                    wrapped_col_name = self._sql_adapter.wrap(col_name)
                    wrapped_pk_col_names[wrapped_col_name] = rows.column_indices[
                        col_name
                    ]
            predicates = []
            for row in rows.as_tuples():
                predicate = " AND ".join(
                    f"{col_name} = {row[ix]}"
                    for col_name, ix in wrapped_pk_col_names.items()
                )
                predicates.append(predicate)
            where_clause = " OR ".join(f"({predicate})" for predicate in predicates)
        if columns:
            select_col_names = [
                self._sql_adapter.wrap(col)
                for col in sorted(self._table.column_names)
                if col in columns
            ]
        else:
            select_col_names = [
                col.wrapped_column_name for col in self._sql_adapter.columns
            ]

        select_cols_csv = ", ".join(select_col_names)
        sql = (
            f"SELECT {select_cols_csv} "
            f"FROM {self.full_table_name} "
            f"WHERE {where_clause}"
        )
        logger.debug(f"Executing SQL:\n\t{sql}")
        return self._connection.execute(sql)

    def keys(self, /, include_change_tracking_cols: bool = True) -> domain.Rows:
        pk_cols_csv = ", ".join(
            self._sql_adapter.wrap(col)
            for col in sorted(self._table.primary_key_column_names)
        )
        if include_change_tracking_cols:
            change_cols_csv = ", ".join(
                col.wrapped_column_name
                for col in self._sql_adapter.columns
                if col.column_metadata.column_name in self._change_tracking_columns
            )
        else:
            change_cols_csv = ""

        if change_cols_csv:
            select_cols_csv = f"{pk_cols_csv}, {change_cols_csv}"
        else:
            select_cols_csv = pk_cols_csv
        sql = (
            f"SELECT DISTINCT {select_cols_csv} "
            f"FROM {self.full_table_name}"
        )
        return self._connection.execute(sql)

    def row_count(self) -> int:
        """Get the number of rows in a table"""
        return self._connection.execute(
            self._sql_adapter.row_count(self._table)
        ).first_value()

    @property
    def table(self) -> domain.Table:
        return self._table

    def update(self, /, rows: domain.Rows) -> None:
        if self._read_only:
            raise exceptions.DatabaseIsReadOnly()

        param_indices = []
        non_pk_col_wrapped_names = []
        for col_name in rows.column_names:
            if col_name not in self._table.primary_key_column_names:
                wrapped_name = self._sql_adapter.wrap(col_name)
                non_pk_col_wrapped_names.append(wrapped_name)
                row_col_index = rows.column_indices[col_name]
                param_indices.append(row_col_index)
        set_clause = ", ".join(
            f"{col_name} = {self._connection.parameter_placeholder(col_name)}"
            for col_name in non_pk_col_wrapped_names
        )
        pk_col_wrapped_names = []
        for col_name in rows.column_names:
            if col_name in self._table.primary_key_column_names:
                wrapped_name = self._sql_adapter.wrap(col_name)
                pk_col_wrapped_names.append(wrapped_name)
                row_col_index = rows.column_indices[col_name]
                param_indices.append(row_col_index)
        where_clause = " AND ".join(
            f"{col_name} = {self._connection.parameter_placeholder(col_name)}"
            for col_name in pk_col_wrapped_names
        )

        sql = (
            f"UPDATE {self.full_table_name} "
            f"SET {set_clause} "
            f"WHERE {where_clause}"
        )
        params = rows.as_dicts()
        return self._connection.execute(sql, params)
