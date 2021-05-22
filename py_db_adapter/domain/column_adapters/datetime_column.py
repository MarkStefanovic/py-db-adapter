import abc
import datetime
import typing

from py_db_adapter.domain import column, column_adapter

__all__ = ("DateTimeColumnSqlAdapter",)


class DateTimeColumnSqlAdapter(
    column_adapter.ColumnSqlAdapter[datetime.datetime], abc.ABC
):
    def __init__(self, *, col: column.Column, wrapper: typing.Callable[[str], str]):
        super().__init__(col=col, wrapper=wrapper)

    @abc.abstractmethod
    def literal(self, value: datetime.datetime) -> str:
        raise NotImplementedError
