import abc
import decimal
import typing

from py_db_adapter.domain import column, column_adapter

__all__ = ("DecimalColumnSqlAdapter",)


class DecimalColumnSqlAdapter(column_adapter.ColumnSqlAdapter[decimal.Decimal], abc.ABC):
    def __init__(
        self,
        *,
        col: column.DecimalColumn,
        wrapper: typing.Callable[[str], str],
    ):
        super().__init__(col=col, wrapper=wrapper)
        self._scale = col.scale
        self._precision = col.precision

    @property
    def column_metadata(self) -> column.DecimalColumn:
        return typing.cast(column.DecimalColumn, super().column_metadata)

    @abc.abstractmethod
    def literal(self, value: decimal.Decimal) -> str:
        raise NotImplementedError

    @property
    def precision(self) -> int:
        return self._precision

    @property
    def scale(self) -> int:
        return self._scale
