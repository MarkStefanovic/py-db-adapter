import datetime
import typing

from py_db_adapter.domain import column, column_adapters

__all__ = ("StandardDateColumnSqlAdapter",)


class StandardDateColumnSqlAdapter(column_adapters.DateColumnSqlAdapter):
    def __init__(self, *, col: column.Column, wrapper: typing.Callable[[str], str]):
        super().__init__(col=col, wrapper=wrapper)

    @property
    def definition(self) -> str:
        return f"{self.wrapped_column_name} DATE {self.nullable}"

    def literal(self, value: datetime.date) -> str:
        date_str = value.strftime("%Y-%m-%d %H:%M:%S")
        return f"CAST({date_str!r} AS DATE)"
