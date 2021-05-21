import decimal
import typing

from py_db_adapter.domain import column, column_adapters

__all__ = ("StandardDecimalColumnSqlAdapter",)


class StandardDecimalColumnSqlAdapter(column_adapters.DecimalColumnSqlAdapter):
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
    def definition(self) -> str:
        return f"{self.wrapped_column_name} DECIMAL({self._precision}, {self._scale}) {self.nullable}"

    def literal(self, value: decimal.Decimal) -> str:
        return f"{float(value):.{self._scale}f}"
