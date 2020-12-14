import typing

from py_db_adapter import domain

__all__ = ("StandardIntegerColumnSqlAdapter",)


class StandardIntegerColumnSqlAdapter(domain.IntegerColumnSqlAdapter):
    def __init__(
        self,
        *,
        col: domain.IntegerColumn,
        wrapper: typing.Callable[[str], str],
    ):
        super().__init__(col=col, wrapper=wrapper)

    @property
    def definition(self) -> str:
        return f"{self.wrapped_column_name} BIGINT {self.nullable}"

    def literal(self, value: int) -> str:
        return str(value)
