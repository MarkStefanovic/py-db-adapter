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
        pk_col_csv = ", ".join(self.wrap(col) for col in sorted(table.primary_key_column_names))
        col_csv = ", ".join(col.definition for col in self.columns(table))
        return sql_formatter.standardize_sql(
            f"""CREATE TABLE {self.full_table_name(table)} (
                {col_csv}
            ,   PRIMARY KEY ({pk_col_csv})
            )"""
        )

    def drop(self, /, table: domain.Table, *, cascade: bool = False) -> str:
        if cascade:
            return f"DROP TABLE {self.full_table_name(table)} CASCADE"
        else:
            return f"DROP TABLE {self.full_table_name(table)}"

    def full_table_name(self, *, schema_name: typing.Optional[str], table_name: str) -> str:
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

    def row_count(self, /, table: domain.Table) -> str:
        return f"SELECT COUNT(*) AS row_count FROM {self.full_table_name(table)}"

    def select_all(self, /, table: domain.Table) -> str:
        return f"SELECT * FROM {self.full_table_name(table)}"

    @abc.abstractmethod
    def table_exists(self, schema_name: typing.Optional[str], table_name: str) -> str:
        raise NotImplementedError

    def truncate(self, /, table: domain.Table) -> str:
        return f"DELETE FROM {self.full_table_name(table)}"

    @abc.abstractmethod
    def wrap(self, obj_name: str) -> str:
        raise NotImplementedError
