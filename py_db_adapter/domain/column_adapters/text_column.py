import abc
import typing

from py_db_adapter.domain import column, column_adapter

__all__ = ("TextColumnSqlAdapter",)


class TextColumnSqlAdapter(column_adapter.ColumnSqlAdapter[str], abc.ABC):
    def __init__(
        self,
        *,
        col: column.TextColumn,
        wrapper: typing.Callable[[str], str],
    ):
        super().__init__(col=col, wrapper=wrapper)

    @property
    def column_metadata(self) -> column.TextColumn:
        return typing.cast(column.TextColumn, super().column_metadata)

    @abc.abstractmethod
    def literal(self, value: str) -> str:
        raise NotImplementedError
