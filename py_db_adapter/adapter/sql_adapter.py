import abc
import functools
import typing

from py_db_adapter import domain
from py_db_adapter.adapter import sql_formatter, column_adapters, column_adapter

__all__ = ("SqlAdapter",)


class SqlAdapter(abc.ABC):
    def __init__(
        self, /, max_float_literal_decimal_places: typing.Optional[int] = None
    ):
        self._max_float_literal_decimal_places = max_float_literal_decimal_places

    def add_rows(
        self, /, parameter_placeholder: typing.Callable[[str], str], rows: domain.Rows
    ) -> str:
        col_name_csv = ",".join(self.wrap(col_name) for col_name in rows.column_names)
        dummy_csv = ",".join(
            parameter_placeholder(col_name) for col_name in rows.column_names
        )
        return (
            f"INSERT INTO {self.full_table_name} ({col_name_csv}) "
            f"VALUES ({dummy_csv})"
        )

    @abc.abstractmethod
    def create_boolean_column(
        self, /, column: domain.BooleanColumn
    ) -> column_adapters.BooleanColumnSqlAdapter:
        raise NotImplementedError

    @functools.cache
    def columns(
        self, /, table: domain.Table
    ) -> typing.List[column_adapter.ColumnSqlAdapter[typing.Any]]:
        col_adapters = []
        for col in table.columns:
            if isinstance(col, domain.BooleanColumn):
                adapter = self.create_boolean_column(col)
            elif isinstance(col, domain.DateColumn):
                adapter = self.create_date_column(col)  # type: ignore
            elif isinstance(col, domain.DateTimeColumn):
                adapter = self.create_datetime_column(col)  # type: ignore
            elif isinstance(col, domain.DecimalColumn):
                adapter = self.create_decimal_column(col)  # type: ignore
            elif isinstance(col, domain.FloatColumn):
                adapter = self.create_float_column(col)  # type: ignore
            elif isinstance(col, domain.IntegerColumn):
                adapter = self.create_integer_column(col)  # type: ignore
            elif isinstance(col, domain.TextColumn):
                adapter = self.create_text_column(col)  # type: ignore
            else:
                raise ValueError(f"Unrecognized col.data_type: {col.data_type!r}")
            col_adapters.append(adapter)

        return sorted(
            col_adapters, key=lambda c: c.column_metadata.column_name  # type: ignore
        )

    @abc.abstractmethod
    def create_date_column(
        self, /, column: domain.DateColumn
    ) -> column_adapters.DateColumnSqlAdapter:
        raise NotImplementedError

    @abc.abstractmethod
    def create_datetime_column(
        self, /, column: domain.DateTimeColumn
    ) -> column_adapters.DateTimeColumnSqlAdapter:
        raise NotImplementedError

    @abc.abstractmethod
    def create_decimal_column(
        self, /, column: domain.DecimalColumn
    ) -> column_adapters.DecimalColumnSqlAdapter:
        raise NotImplementedError

    @abc.abstractmethod
    def create_float_column(
        self, /, column: domain.FloatColumn
    ) -> column_adapters.FloatColumnSqlAdapter:
        raise NotImplementedError

    @abc.abstractmethod
    def create_integer_column(
        self, /, column: domain.IntegerColumn
    ) -> column_adapters.IntegerColumnSqlAdapter:
        raise NotImplementedError

    @abc.abstractmethod
    def create_text_column(
        self, /, column: domain.TextColumn
    ) -> column_adapters.TextColumnSqlAdapter:
        raise NotImplementedError

    def definition(self, /, table: domain.Table) -> str:
        pk_col_csv = ", ".join(
            self.wrap(col) for col in sorted(table.primary_key_column_names)
        )
        col_csv = ", ".join(col.definition for col in self.columns(table))
        full_table_name = self.full_table_name(
            schema_name=table.schema_name, table_name=table.table_name
        )
        return sql_formatter.standardize_sql(
            f"""CREATE TABLE {full_table_name} (
                {col_csv}
            ,   PRIMARY KEY ({pk_col_csv})
            )"""
        )

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
        return f"DELETE FROM {full_table_name} " f"WHERE {where_clause}"

    def drop(
        self,
        *,
        schema_name: typing.Optional[str],
        table_name: str,
        cascade: bool = False,
    ) -> str:
        full_table_name = self.full_table_name(
            schema_name=schema_name, table_name=table_name
        )
        if cascade:
            return f"DROP TABLE {full_table_name} CASCADE"
        else:
            return f"DROP TABLE {full_table_name}"

    def fetch_rows_by_primary_key_values(
        self,
        *,
        cols: typing.Optional[typing.Set[column_adapter.ColumnSqlAdapter[typing.Any]]],
        rows: domain.Rows,
    ) -> str:
        pk_cols = sorted(col for col in cols if col.column_metadata.primary_key)
        if len(pk_cols) == 1:
            pk_col: column_adapter.ColumnSqlAdapter[typing.Any] = pk_cols[0]
            wrapped_pk_col_name = pk_col.wrapped_column_name
            pk_values = rows.column(pk_col.column_metadata.column_name)
            pk_values_csv = ",".join(pk_col.literal(v) for v in pk_values)
            where_clause = f"{wrapped_pk_col_name} IN ({pk_values_csv})"
        else:
            wrapped_pk_col_names = {}
            for col_name in rows.column_names:
                if col_name in pk_cols:
                    wrapped_col_name = self.wrap(col_name)
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

        if cols:
            select_col_names = [
                col.wrapped_column_name
                for col in sorted(cols, key=lambda c: c.column_metadata.column_name)
                if col in columns
            ]
        else:
            select_col_names = [
                col.wrapped_column_name for col in cols
            ]

        select_cols_csv = ", ".join(select_col_names)
        sql = (
            f"SELECT {select_cols_csv} "
            f"FROM {self.full_table_name} "
            f"WHERE {where_clause}"
        )

    def full_table_name(
        self, *, schema_name: typing.Optional[str], table_name: str
    ) -> str:
        if schema_name is None:
            return self.wrap(table_name)
        else:
            return f"{self.wrap(schema_name)}.{self.wrap(table_name)}"

    @property
    def max_float_literal_decimal_places(self) -> int:
        return self._max_float_literal_decimal_places or 5

    def primary_key_columns(
        self, /, table: domain.Table
    ) -> typing.List[column_adapter.ColumnSqlAdapter[typing.Any]]:
        return [col for col in self.columns(table) if col.column_metadata.primary_key]

    def row_count(self, *, schema_name: typing.Optional[str], table_name: str) -> str:
        full_table_name = self.full_table_name(
            schema_name=schema_name, table_name=table_name
        )
        return f"SELECT COUNT(*) AS row_count FROM {full_table_name}"

    def select_all(self, *, schema_name: typing.Optional[str], table_name: str) -> str:
        full_table_name = self.full_table_name(
            schema_name=schema_name, table_name=table_name
        )
        return f"SELECT * FROM {full_table_name}"

    def select_keys(
        self,
        *,
        pk_cols: typing.Set[str],
        change_tracking_cols: typing.Set[str],
        include_change_tracking_cols: bool = True,
    ) -> str:
        pk_cols_csv = ", ".join(self.wrap(col) for col in sorted(pk_cols))
        if include_change_tracking_cols:
            change_cols_csv = ",".join(
                col.wrapped_column_name
                for col in self.columns
                if col.column_metadata.column_name in change_tracking_cols
            )
        else:
            change_cols_csv = ""

        if change_cols_csv:
            select_cols_csv = f"{pk_cols_csv}, {change_cols_csv}"
        else:
            select_cols_csv = pk_cols_csv
        return f"SELECT DISTINCT {select_cols_csv} " f"FROM {self.full_table_name}"

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

    @abc.abstractmethod
    def wrap(self, obj_name: str) -> str:
        raise NotImplementedError
