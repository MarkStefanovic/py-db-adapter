import abc
import datetime
import typing

from py_db_adapter import domain
from py_db_adapter.adapter.sql_adapter.sql_column_adapter import column_sql_adapter


__all__ = (
    "DateColumnSqlAdapter",
    "StandardDateColumnSqlAdapter",
)


class DateColumnSqlAdapter(
    column_sql_adapter.ColumnSqlAdapter[typing.Type[datetime.date]], abc.ABC
):
    def __init__(
        self, *, column: domain.DateColumn, wrapper: typing.Callable[[str], str]
    ):
        super().__init__(column=column, wrapper=wrapper)

    @property
    def column_metadata(self) -> domain.DateColumn:
        return typing.cast(domain.DateColumn, super().column_metadata)

    @abc.abstractmethod
    def literal(self, value: datetime.date) -> str:
        raise NotImplementedError


class StandardDateColumnSqlAdapter(DateColumnSqlAdapter):
    def __init__(
        self, *, column: domain.DateColumn, wrapper: typing.Callable[[str], str]
    ):
        super().__init__(column=column, wrapper=wrapper)

    @property
    def definition(self) -> str:
        return f"{self.wrapped_column_name} DATE {self.nullable}"

    def literal(self, value: datetime.datetime) -> str:
        date_str = value.strftime("%Y-%m-%d %H:%M:%S")
        return f"CAST({date_str!r} AS DATE)"
