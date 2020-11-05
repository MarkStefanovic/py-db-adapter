import abc
import typing

from py_db_adapter import domain
from py_db_adapter.adapter.sql_adapter import sql_column_adapter
from py_db_adapter.adapter import sql_formatter

__all__ = (
    "Row",
    "Rows",
    "SqlTableAdapter",
)


Row = typing.Dict[str, typing.Hashable]
Rows = typing.Collection[Row]


class SqlTableAdapter(abc.ABC):
    def __init__(
        self,
        *,
        table: domain.Table,
        max_float_literal_decimal_places: typing.Optional[int] = None,
    ):
        self._table = table
        self._max_float_literal_decimal_places = max_float_literal_decimal_places

        self._column_sql_adapters: typing.Optional[typing.List[domain.Column]] = None

    @abc.abstractmethod
    def create_boolean_column_sql_adapter(
        self, /, column: domain.BooleanColumn
    ) -> sql_column_adapter.BooleanColumnSqlAdapter:
        raise NotImplementedError

    @property
    def column_sql_adapters(
        self,
    ) -> typing.List[sql_column_adapter.AnyColumnSqlAdapter]:
        if self._column_sql_adapters is None:
            col_adapters = []
            for col in self._table.columns:
                if isinstance(col, domain.BooleanColumn):
                    adapter = self.create_boolean_column_sql_adapter(col)
                elif isinstance(col, domain.DateColumn):
                    adapter = self.create_date_column_sql_adapter(col)  # type: ignore
                elif isinstance(col, domain.DateTimeColumn):
                    adapter = self.create_datetime_column_sql_adapter(col)  # type: ignore
                elif isinstance(col, domain.DecimalColumn):
                    adapter = self.create_decimal_column_sql_adapter(col)  # type: ignore
                elif isinstance(col, domain.FloatColumn):
                    adapter = self.create_float_column_sql_adapter(col)  # type: ignore
                elif isinstance(col, domain.IntegerColumn):
                    adapter = self.create_integer_column_sql_adapter(col)  # type: ignore
                elif isinstance(col, domain.TextColumn):
                    adapter = self.create_text_column_sql_adapter(col)  # type: ignore
                else:
                    raise ValueError(f"Unrecognized col.data_type: {col.data_type!r}")
                col_adapters.append(adapter)

            self._column_sql_adapters = sorted(
                col_adapters, key=lambda c: c.column_metadata.column_name  # type: ignore
            )
        return self._column_sql_adapters  # type: ignore

    @abc.abstractmethod
    def create_date_column_sql_adapter(
        self, /, column: domain.DateColumn
    ) -> sql_column_adapter.DateColumnSqlAdapter:
        raise NotImplementedError

    @abc.abstractmethod
    def create_datetime_column_sql_adapter(
        self, /, column: domain.DateTimeColumn
    ) -> sql_column_adapter.DateTimeColumnSqlAdapter:
        raise NotImplementedError

    @abc.abstractmethod
    def create_decimal_column_sql_adapter(
        self, /, column: domain.DecimalColumn
    ) -> sql_column_adapter.DecimalColumnSqlAdapter:
        raise NotImplementedError

    @abc.abstractmethod
    def create_float_column_sql_adapter(
        self, /, column: domain.FloatColumn
    ) -> sql_column_adapter.FloatColumnSqlAdapter:
        raise NotImplementedError

    @abc.abstractmethod
    def create_integer_column_sql_adapter(
        self, /, column: domain.IntegerColumn
    ) -> sql_column_adapter.IntegerColumnSqlAdapter:
        raise NotImplementedError

    @abc.abstractmethod
    def create_text_column_sql_adapter(
        self, /, column: domain.TextColumn
    ) -> sql_column_adapter.TextColumnSqlAdapter:
        raise NotImplementedError

    @property
    def create(self) -> str:
        pk_col_names = sorted(
            col.column_name for col in self.table_metadata.columns if col.primary_key
        )
        pk_col_csv = ", ".join(self.wrap(col) for col in pk_col_names)
        col_csv = ", ".join(
            col.definition
            for col in self.column_sql_adapters
        )
        return sql_formatter.standardize_sql(
            f"""CREATE TABLE {self.full_table_name} (
                {col_csv}
            ,   PRIMARY KEY ({pk_col_csv})
            )"""
        )

    @property
    def drop(self) -> str:
        return f"DROP TABLE {self.full_table_name}"

    @property
    def full_table_name(self) -> str:
        if self._table.schema_name is None:
            return self.wrap(self._table.table_name)
        else:
            return f"{self.wrap(self._table.schema_name)}.{self.wrap(self._table.table_name)}"

    @property
    def max_float_literal_decimal_places(self) -> int:
        return self._max_float_literal_decimal_places or 5

    @property
    def primary_key_column_sql_adapters(self) -> typing.List[sql_column_adapter.AnyColumnSqlAdapter]:
        return [col for col in self.column_sql_adapters if col.column_metadata.primary_key]

    @property
    def row_count(self) -> str:
        return f"SELECT COUNT(*) AS row_count FROM {self.full_table_name}"

    @property
    def table_metadata(self) -> domain.Table:
        return self._table

    @property
    def truncate(self) -> str:
        return f"DELETE FROM {self.full_table_name}"

    @abc.abstractmethod
    def wrap(self, obj_name: str) -> str:
        raise NotImplementedError
