import abc
import decimal
import typing

from py_db_adapter import domain

__all__ = (
    "DecimalColumnSqlAdapter",
    "StandardDecimalColumnSqlAdapter",
)


class DecimalColumnSqlAdapter(domain.ColumnSqlAdapter[decimal.Decimal], abc.ABC):
    def __init__(
        self,
        *,
        column: domain.DecimalColumn,
        wrapper: typing.Callable[[str], str],
    ):
        super().__init__(column=column, wrapper=wrapper)
        self._scale = column.scale
        self._precision = column.precision

    @property
    def column_metadata(self) -> domain.DecimalColumn:
        return typing.cast(domain.DecimalColumn, super().column_metadata)

    @abc.abstractmethod
    def literal(self, value: decimal.Decimal) -> str:
        raise NotImplementedError

    @property
    def precision(self) -> int:
        return self._precision

    @property
    def scale(self) -> int:
        return self._scale


class StandardDecimalColumnSqlAdapter(DecimalColumnSqlAdapter):
    def __init__(
        self,
        *,
        column: domain.DecimalColumn,
        wrapper: typing.Callable[[str], str],
    ):
        super().__init__(column=column, wrapper=wrapper)
        self._scale = column.scale
        self._precision = column.precision

    @property
    def definition(self) -> str:
        return f"{self.wrapped_column_name} DECIMAL({self._precision}, {self._scale}) {self.nullable}"

    def literal(self, value: decimal.Decimal) -> str:
        return f"{float(value):.{self._scale}f}"
