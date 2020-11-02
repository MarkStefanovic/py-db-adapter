import abc
import datetime

import typing

from py_db_adapter import domain
from py_db_adapter.adapter.sql_adapter.sql_column_adapter import column_sql_adapter


__all__ = (
    "DateTimeColumnSqlAdapter",
    "StandardDateTimeColumnSqlAdapter",
)


class DateTimeColumnSqlAdapter(
    column_sql_adapter.ColumnSqlAdapter[typing.Type[datetime.datetime]], abc.ABC
):
    def __init__(
        self, *, column: domain.DateTimeColumn, wrapper: typing.Callable[[str], str]
    ):
        super().__init__(column=column, wrapper=wrapper)

    @property
    def column_metadata(self) -> domain.DateTimeColumn:
        return typing.cast(domain.DateTimeColumn, super().column_metadata)

    @abc.abstractmethod
    def literal(self, value: datetime.datetime) -> str:
        raise NotImplementedError


class StandardDateTimeColumnSqlAdapter(DateTimeColumnSqlAdapter):
    def __init__(
        self, *, column: domain.DateTimeColumn, wrapper: typing.Callable[[str], str]
    ):
        super().__init__(column=column, wrapper=wrapper)

    @property
    def definition(self) -> str:
        return f"{self.wrapped_column_name} TIMESTAMP {self.nullable}"

    def literal(self, value: datetime.datetime) -> str:
        date_str = value.strftime("%Y-%m-%d %H:%M:%S")
        return f"CAST({date_str!r} AS TIMESTAMP)"
