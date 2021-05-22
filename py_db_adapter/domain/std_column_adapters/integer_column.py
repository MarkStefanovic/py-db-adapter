import typing

from py_db_adapter.domain import column, column_adapters

__all__ = ("StandardIntegerColumnSqlAdapter",)


class StandardIntegerColumnSqlAdapter(column_adapters.IntegerColumnSqlAdapter):
    def __init__(
        self,
        *,
        col: column.Column,
        wrapper: typing.Callable[[str], str],
    ):
        super().__init__(col=col, wrapper=wrapper)

    @property
    def definition(self) -> str:
        return f"{self.wrapped_column_name} BIGINT {self.nullable}"

    def literal(self, value: int) -> str:
        return str(value)
