import abc
import typing

from py_db_adapter.domain import column, column_adapter

__all__ = ("BooleanColumnSqlAdapter",)


class BooleanColumnSqlAdapter(column_adapter.ColumnSqlAdapter[bool], abc.ABC):
    def __init__(
        self,
        *,
        col: column.BooleanColumn,
        wrapper: typing.Callable[[str], str],
    ):
        super().__init__(col=col, wrapper=wrapper)

    @property
    def column_metadata(self) -> column.BooleanColumn:
        return typing.cast(column.BooleanColumn, super().column_metadata)

    @abc.abstractmethod
    def literal(self, value: bool) -> str:
        raise NotImplementedError
