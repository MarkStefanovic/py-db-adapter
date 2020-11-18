from __future__ import annotations

import logging
import typing

from py_db_adapter import domain, adapter
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
        else:
            sql = self._sql_adapter.add_rows(
                parameter_placeholder=self._connection.parameter_placeholder,
                rows=rows,
            )
            params = rows.as_dicts()
            self._connection.execute(sql, params=params)

    def all(self) -> domain.Rows:
        sql = self._sql_adapter.select_all(schema_name=self.table.schema_name, table_name=self.table.table_name)
        return self._connection.execute(sql)

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
        else:
            sql = self._sql_adapter.delete(
                schema_name=self._table.schema_name,
                table_name=self._table.table_name,
                pk_cols=self._table.primary_key_column_names,
                parameter_placeholder=self._connection.parameter_placeholder,
                row_cols=rows.column_names,
            )
            params = rows.as_dicts()
            self._connection.execute(sql, params)

    def drop(self, /, cascade: bool = False) -> None:
        if self._read_only:
            raise exceptions.DatabaseIsReadOnly()
        else:
            sql = self._sql_adapter.drop(
                schema_name=self._table.schema_name,
                table_name=self._table.table_name,
                cascade=cascade,
            )
            self._connection.execute(sql)

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
        if include_change_tracking_cols:
            sql = self._sql_adapter.select_keys(
                pk_cols=self._table.primary_key_column_names,
                change_tracking_cols=set(self._change_tracking_columns),
                include_change_tracking_cols=include_change_tracking_cols,
            )
        else:
            sql = self._sql_adapter.select_keys(
                pk_cols=self._table.primary_key_column_names,
                change_tracking_cols=set(),
                include_change_tracking_cols=include_change_tracking_cols,
            )
        return self._connection.execute(sql)

    def row_count(self) -> int:
        """Get the number of rows in a table"""
        return self._connection.execute(
            self._sql_adapter.row_count(schema_name=self.schema_name, table_name=self.table_name)
        ).first_value()

    @property
    def table(self) -> domain.Table:
        return self._table

    @property
    def table_name(self) -> str:
        return self._

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

    def upsert_rows(
        self,
        *,
        rows: domain.Rows,
        add: bool = True,
        update: bool = True,
        delete: bool = True,
    ) -> None:
        src_keys = rows.subset(
            key_columns=common_key_cols,
            value_columns: typing.Optional[typing.Set[str]] = None,
        )
        changes = domain.compare_rows(
            key_cols=self.table.primary_key_column_names,
            src_rows=src_keys,
            dest_rows=self.keys(True),
        )
        common_cols = self.table.column_names.intersection(
            rows.column_names
        )
        if changes["added"].row_count and add:
            new_rows = self.fetch_rows_by_primary_key_values(
                rows=changes["added"], columns=common_cols
            )
            self.add(new_rows)
        if changes["deleted"].row_count and delete:
            self.delete(changes["deleted"])
        if changes["updated"].row_count and update:
            updated_rows = self.fetch_rows_by_primary_key_values(
                rows=changes["updated"], columns=common_cols
            )
            self.update(updated_rows)