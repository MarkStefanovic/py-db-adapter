import abc
import datetime
import typing

from py_db_adapter.domain import column, column_adapter

__all__ = ("DateColumnSqlAdapter",)


class DateColumnSqlAdapter(column_adapter.ColumnSqlAdapter[datetime.date], abc.ABC):
    def __init__(self, *, col: column.Column, wrapper: typing.Callable[[str], str]):
        super().__init__(col=col, wrapper=wrapper)

    @abc.abstractmethod
    def literal(self, value: datetime.date) -> str:
        raise NotImplementedError
