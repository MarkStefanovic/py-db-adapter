import typing

from py_db_adapter.domain import column, column_adapters

__all__ = ("StandardBooleanColumnSqlAdapter",)


class StandardBooleanColumnSqlAdapter(column_adapters.BooleanColumnSqlAdapter):
    def __init__(self, *, col: column.Column, wrapper: typing.Callable[[str], str]):
        super().__init__(col=col, wrapper=wrapper)

    @property
    def definition(self) -> str:
        return f"{self.wrapped_column_name} INT {self.nullable}"

    def literal(self, value: bool) -> str:
        return "1" if value else "0"
