import datetime
import typing

from py_db_adapter import domain

__all__ = ("StandardDateTimeColumnSqlAdapter",)


class StandardDateTimeColumnSqlAdapter(domain.DateTimeColumnSqlAdapter):
    def __init__(
        self, *, col: domain.DateTimeColumn, wrapper: typing.Callable[[str], str]
    ):
        super().__init__(col=col, wrapper=wrapper)

    @property
    def definition(self) -> str:
        return f"{self.wrapped_column_name} TIMESTAMP {self.nullable}"

    def literal(self, value: datetime.datetime) -> str:
        date_str = value.strftime("%Y-%m-%d %H:%M:%S")
        return f"CAST({date_str!r} AS TIMESTAMP)"
