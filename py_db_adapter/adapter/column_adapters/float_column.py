import abc

import typing

from py_db_adapter import domain

__all__ = (
    "FloatColumnSqlAdapter",
    "StandardFloatColumnSqlAdapter",
)


class FloatColumnSqlAdapter(domain.ColumnSqlAdapter[float], abc.ABC):
    def __init__(
        self,
        *,
        column: domain.FloatColumn,
        wrapper: typing.Callable[[str], str],
        max_decimal_places: typing.Optional[int],
    ):
        super().__init__(column=column, wrapper=wrapper)
        self._max_decimal_places = max_decimal_places

    @property
    def column_metadata(self) -> domain.FloatColumn:
        return typing.cast(domain.FloatColumn, super().column_metadata)

    def literal(self, value: float) -> str:
        raise NotImplementedError

    @property
    def max_decimal_places(self) -> typing.Optional[int]:
        return self._max_decimal_places


class StandardFloatColumnSqlAdapter(FloatColumnSqlAdapter):
    def __init__(
        self,
        *,
        column: domain.FloatColumn,
        wrapper: typing.Callable[[str], str],
        max_decimal_places: typing.Optional[int],
    ):
        super().__init__(
            column=column,
            wrapper=wrapper,
            max_decimal_places=max_decimal_places,
        )
        self._max_decimal_places = max_decimal_places

    @property
    def definition(self) -> str:
        return f"{self.wrapped_column_name} FLOAT {self.nullable}"

    def literal(self, value: float) -> str:
        # noinspection PyStringFormat
        return f"{{:.{self._max_decimal_places}f}}".format(value)
