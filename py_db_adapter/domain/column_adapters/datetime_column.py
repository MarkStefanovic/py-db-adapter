import abc
import datetime
import typing

from py_db_adapter.domain import column, column_adapter

__all__ = ("DateTimeColumnSqlAdapter",)


class DateTimeColumnSqlAdapter(
    column_adapter.ColumnSqlAdapter[datetime.datetime], abc.ABC
):
    def __init__(
        self, *, col: column.DateTimeColumn, wrapper: typing.Callable[[str], str]
    ):
        super().__init__(col=col, wrapper=wrapper)

    @property
    def column_metadata(self) -> column.DateTimeColumn:
        return typing.cast(column.DateTimeColumn, super().column_metadata)

    @abc.abstractmethod
    def literal(self, value: datetime.datetime) -> str:
        raise NotImplementedError
