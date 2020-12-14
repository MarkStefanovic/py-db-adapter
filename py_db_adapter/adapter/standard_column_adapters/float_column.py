import typing

from py_db_adapter import domain

__all__ = ("StandardFloatColumnSqlAdapter",)


class StandardFloatColumnSqlAdapter(domain.FloatColumnSqlAdapter):
    def __init__(
        self,
        *,
        col: domain.FloatColumn,
        wrapper: typing.Callable[[str], str],
        max_decimal_places: typing.Optional[int],
    ):
        super().__init__(
            col=col,
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
