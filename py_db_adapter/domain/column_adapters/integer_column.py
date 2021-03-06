import abc
import typing

from py_db_adapter.domain import column, column_adapter

__all__ = ("IntegerColumnSqlAdapter",)


class IntegerColumnSqlAdapter(column_adapter.ColumnSqlAdapter[int], abc.ABC):
    def __init__(
        self,
        *,
        col: column.Column,
        wrapper: typing.Callable[[str], str],
    ):
        super().__init__(col=col, wrapper=wrapper)

    @abc.abstractmethod
    def literal(self, value: int) -> str:
        raise NotImplementedError
