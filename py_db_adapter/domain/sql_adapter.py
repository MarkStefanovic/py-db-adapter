import abc
import typing

from py_db_adapter.domain import (
    column as domain_column,
    column_adapter as domain_column_adapter,
    column_adapters as domain_column_adapters,
    rows as domain_rows,
    sql_operator,
    sql_predicate,
    table as domain_table,
)

__all__ = ("SqlAdapter",)


class SqlAdapter(abc.ABC):
    def __init__(self, /, max_float_literal_decimal_places: int = 5) -> None:
        self._max_float_literal_decimal_places = max_float_literal_decimal_places

    def add_rows(
        self,
        *,
        schema_name: typing.Optional[str],
        table_name: str,
        parameter_placeholder: typing.Callable[[str], str],
        rows: domain_rows.Rows,
    ) -> str:
        col_name_csv = ",".join(
            self.wrap(col_name) for col_name in sorted(rows.column_names)
        )
        dummy_csv = ",".join(
            parameter_placeholder(col_name) for col_name in rows.column_names
        )
        full_table_name = self.full_table_name(
            schema_name=schema_name, table_name=table_name
        )
        return f"INSERT INTO {full_table_name} ({col_name_csv}) VALUES ({dummy_csv})"

    @abc.abstractmethod
    def create_boolean_column(
        self, /, column: domain_column.BooleanColumn
    ) -> domain_column_adapters.BooleanColumnSqlAdapter:
        raise NotImplementedError

    @abc.abstractmethod
    def create_date_column(
        self, /, column: domain_column.DateColumn
    ) -> domain_column_adapters.DateColumnSqlAdapter:
        raise NotImplementedError

    @abc.abstractmethod
    def create_datetime_column(
        self, /, column: domain_column.DateTimeColumn
    ) -> domain_column_adapters.DateTimeColumnSqlAdapter:
        raise NotImplementedError

    @abc.abstractmethod
    def create_decimal_column(
        self, /, column: domain_column.DecimalColumn
    ) -> domain_column_adapters.DecimalColumnSqlAdapter:
        raise NotImplementedError

    @abc.abstractmethod
    def create_float_column(
        self, /, column: domain_column.FloatColumn
    ) -> domain_column_adapters.FloatColumnSqlAdapter:
        raise NotImplementedError

    @abc.abstractmethod
    def create_integer_column(
        self, /, column: domain_column.IntegerColumn
    ) -> domain_column_adapters.IntegerColumnSqlAdapter:
        raise NotImplementedError

    @abc.abstractmethod
    def create_text_column(
        self, /, column: domain_column.TextColumn
    ) -> domain_column_adapters.TextColumnSqlAdapter:
        raise NotImplementedError

    def definition(self, /, table: domain_table.Table) -> str:
        pk_col_csv = ", ".join(self.wrap(col) for col in sorted(table.pk_cols))
        adapters = sorted(
            (self._map_column_to_adapter(col) for col in table.columns),
            key=lambda c: c.column_metadata.column_name,
        )
        col_csv = ", ".join(col.definition for col in adapters)
        full_table_name = self.full_table_name(
            schema_name=table.schema_name, table_name=table.table_name
        )
        return f"CREATE TABLE {full_table_name} ({col_csv}, PRIMARY KEY ({pk_col_csv}))"

    def delete(
        self,
        *,
        schema_name: typing.Optional[str],
        table_name: str,
        pk_cols: typing.Set[str],
        parameter_placeholder: typing.Callable[[str], str],
        row_cols: typing.List[str],
    ) -> str:
        where_clause = " AND ".join(
            f"{self.wrap(col_name)} = {parameter_placeholder(col_name)}"
            for col_name in row_cols
            if col_name in pk_cols
        )
        full_table_name = self.full_table_name(
            schema_name=schema_name, table_name=table_name
        )
        return f"DELETE FROM {full_table_name} WHERE {where_clause}"

    def drop(
        self,
        *,
        schema_name: typing.Optional[str],
        table_name: str,
    ) -> str:
        full_table_name = self.full_table_name(
            schema_name=schema_name, table_name=table_name
        )
        return f"DROP TABLE {full_table_name}"

    @abc.abstractmethod
    def fast_row_count(
        self, *, schema_name: typing.Optional[str], table_name: str
    ) -> str:
        raise NotImplementedError

    def fetch_rows_by_primary_key_values(
        self,
        *,
        schema_name: typing.Optional[str],
        table_name: str,
        rows: domain_rows.Rows,
        pk_cols: typing.Set[domain_column.Column],
        select_cols: typing.Optional[typing.Set[str]],
    ) -> str:
        # where_clause = self._where_clause(rows=rows, pk_cols=pk_cols)
        if len(pk_cols) == 1:
            sorted_pk_cols = sorted(pk_cols, key=lambda c: c.column_name)
            pk_col = sorted_pk_cols[0]
            pk_col_adapter = self._map_column_to_adapter(pk_col)
            wrapped_pk_col_name = pk_col_adapter.wrapped_column_name
            pk_values = rows.column(pk_col_adapter.column_metadata.column_name)
            pk_values_csv = ",".join(pk_col_adapter.literal(v) for v in pk_values)
            where_clause = f"{wrapped_pk_col_name} IN ({pk_values_csv})"
        else:
            pk_col_adapters = sorted(
                (self._map_column_to_adapter(col) for col in pk_cols),
                key=lambda c: c.column_metadata.column_name,
            )
            row_identifiers: typing.List[typing.Dict[str, typing.Any]] = []
            for row in rows.as_dicts():
                row_identifier: typing.Dict[str, typing.Any] = {}
                for col_adapter in pk_col_adapters:
                    col_name = col_adapter.column_metadata.column_name
                    row_identifier[
                        col_adapter.wrapped_column_name
                    ] = col_adapter.literal(row[col_name])
                row_identifiers.append(row_identifier)

            row_predicates: typing.List[str] = []
            for row_identifier in row_identifiers:
                row_predicate = " AND ".join(
                    f"{wrapped_col_name} = {literal_val}"
                    for wrapped_col_name, literal_val in row_identifier.items()
                )
                row_predicates.append(row_predicate)

            where_clause = " OR ".join(
                f"({row_predicate})" for row_predicate in row_predicates
            )

        if select_cols:
            select_col_names = [self.wrap(col) for col in sorted(select_cols)]
            select_clause = ", ".join(select_col_names)
        else:
            select_clause = "*"

        full_table_name = self.full_table_name(
            schema_name=schema_name,
            table_name=table_name,
        )

        return f"SELECT {select_clause} FROM {full_table_name} WHERE {where_clause}"

    def full_table_name(
        self, *, schema_name: typing.Optional[str], table_name: str
    ) -> str:
        if schema_name is None:
            return self.wrap(table_name)
        else:
            return f"{self.wrap(schema_name)}.{self.wrap(table_name)}"

    def _map_column_to_adapter(
        self, /, col: domain_column.Column
    ) -> domain_column_adapter.ColumnSqlAdapter[typing.Any]:
        if isinstance(col, domain_column.BooleanColumn):
            return self.create_boolean_column(col)
        elif isinstance(col, domain_column.DateColumn):
            return self.create_date_column(col)
        elif isinstance(col, domain_column.DateTimeColumn):
            return self.create_datetime_column(col)
        elif isinstance(col, domain_column.DecimalColumn):
            return self.create_decimal_column(col)
        elif isinstance(col, domain_column.FloatColumn):
            return self.create_float_column(col)
        elif isinstance(col, domain_column.IntegerColumn):
            return self.create_integer_column(col)
        elif isinstance(col, domain_column.TextColumn):
            return self.create_text_column(col)
        else:
            raise ValueError(f"Unrecognized col.data_type: {col.data_type!r}")

    @property
    def max_float_literal_decimal_places(self) -> int:
        return self._max_float_literal_decimal_places

    def primary_key_columns(
        self, /, table: domain_table.Table
    ) -> typing.List[domain_column_adapter.ColumnSqlAdapter[typing.Any]]:
        return [
            self._map_column_to_adapter(col)
            for col in table.columns
            if col.column_name in table.pk_cols
        ]

    def row_count(self, *, schema_name: typing.Optional[str], table_name: str) -> str:
        full_table_name = self.full_table_name(
            schema_name=schema_name, table_name=table_name
        )
        return f"SELECT COUNT(*) AS row_count FROM {full_table_name}"

    def select_all(
        self,
        *,
        schema_name: typing.Optional[str],
        table_name: str,
        columns: typing.Optional[typing.Set[str]] = None,
    ) -> str:
        full_table_name = self.full_table_name(
            schema_name=schema_name, table_name=table_name
        )
        if columns:
            col_name_csv = ", ".join(self.wrap(col) for col in columns)
            return f"SELECT {col_name_csv} FROM {full_table_name}"
        else:
            return f"SELECT * FROM {full_table_name}"

    def select_distinct(
        self,
        *,
        schema_name: typing.Optional[str],
        table_name: str,
        columns: typing.Set[str],
    ) -> str:
        col_names_csv = ",".join(self.wrap(col) for col in columns)
        full_table_name = self.full_table_name(
            schema_name=schema_name, table_name=table_name
        )
        return f"SELECT DISTINCT {col_names_csv} FROM {full_table_name}"

    def select_where(
        self, *, table: domain_table.Table, predicate: sql_predicate.SqlPredicate
    ) -> str:
        col_names_csv = ",".join(self.wrap(col) for col in table.column_names)
        full_table_name = self.full_table_name(
            schema_name=table.schema_name, table_name=table.table_name
        )
        col_adapter = self._map_column_to_adapter(table.column(predicate.column_name))
        if predicate.operator == sql_operator.SqlOperator.EQUALS:
            pred_sql = f"{self.wrap(predicate.column_name)} = {col_adapter.literal(predicate.value)}"
        else:
            # TODO implement other operators, as well as AND and OR
            raise NotImplementedError
        return f"SELECT {col_names_csv} FROM {full_table_name} WHERE {pred_sql}"

    @abc.abstractmethod
    def table_exists(
        self, *, schema_name: typing.Optional[str], table_name: str
    ) -> str:
        raise NotImplementedError

    def truncate(self, *, schema_name: typing.Optional[str], table_name: str) -> str:
        full_table_name = self.full_table_name(
            schema_name=schema_name, table_name=table_name
        )
        return f"DELETE FROM {full_table_name}"

    def update(
        self,
        *,
        schema_name: typing.Optional[str],
        table_name: str,
        pk_cols: typing.Set[str],
        column_names: typing.Set[str],
        parameter_placeholder: typing.Callable[[str], str],
    ) -> str:
        non_pk_cols = {col for col in column_names if col not in pk_cols}
        non_pk_col_wrapped_names = [self.wrap(col) for col in sorted(non_pk_cols)]
        set_clause = ", ".join(
            f"{col_name} = {parameter_placeholder(col_name)}"
            for col_name in non_pk_col_wrapped_names
        )
        full_table_name = self.full_table_name(
            schema_name=schema_name, table_name=table_name
        )
        where_clause = " AND ".join(f"{self.wrap(col)} = ?" for col in sorted(pk_cols))
        return f"UPDATE {full_table_name} SET {set_clause} WHERE {where_clause}"

    @abc.abstractmethod
    def wrap(self, obj_name: str) -> str:
        raise NotImplementedError
