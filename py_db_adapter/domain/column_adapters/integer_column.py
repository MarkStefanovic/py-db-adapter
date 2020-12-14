import abc
import typing

from py_db_adapter.domain import column, column_adapter

__all__ = ("IntegerColumnSqlAdapter",)


class IntegerColumnSqlAdapter(column_adapter.ColumnSqlAdapter[int], abc.ABC):
    def __init__(
        self,
        *,
        col: column.IntegerColumn,
        wrapper: typing.Callable[[str], str],
    ):
        super().__init__(col=col, wrapper=wrapper)

    @property
    def column_metadata(self) -> column.Column:
        return typing.cast(column.IntegerColumn, super().column_metadata)

    @abc.abstractmethod
    def literal(self, value: int) -> str:
        raise NotImplementedError
