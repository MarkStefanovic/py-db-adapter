import abc
import decimal
import typing

from py_db_adapter.domain import column, column_adapter

__all__ = ("DecimalColumnSqlAdapter",)


class DecimalColumnSqlAdapter(
    column_adapter.ColumnSqlAdapter[decimal.Decimal], abc.ABC
):
    def __init__(
        self,
        *,
        col: column.Column,
        wrapper: typing.Callable[[str], str],
    ):
        super().__init__(col=col, wrapper=wrapper)
        self._scale = col.scale
        self._precision = col.precision

    @abc.abstractmethod
    def literal(self, value: decimal.Decimal) -> str:
        raise NotImplementedError

    @property
    def precision(self) -> int:
        assert self._precision is not None
        return self._precision

    @property
    def scale(self) -> int:
        assert self._scale is not None
        return self._scale
