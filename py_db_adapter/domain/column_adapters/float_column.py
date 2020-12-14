import abc

import typing

from py_db_adapter.domain import column, column_adapter

__all__ = ("FloatColumnSqlAdapter",)


class FloatColumnSqlAdapter(column_adapter.ColumnSqlAdapter[float], abc.ABC):
    def __init__(
        self,
        *,
        col: column.FloatColumn,
        wrapper: typing.Callable[[str], str],
        max_decimal_places: typing.Optional[int],
    ):
        super().__init__(col=col, wrapper=wrapper)
        self._max_decimal_places = max_decimal_places

    @property
    def column_metadata(self) -> column.FloatColumn:
        return typing.cast(column.FloatColumn, super().column_metadata)

    def literal(self, value: float) -> str:
        raise NotImplementedError

    @property
    def max_decimal_places(self) -> typing.Optional[int]:
        return self._max_decimal_places
