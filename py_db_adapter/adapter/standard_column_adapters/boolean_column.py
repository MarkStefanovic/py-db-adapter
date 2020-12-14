import typing

from py_db_adapter import domain

__all__ = ("StandardBooleanColumnSqlAdapter",)


class StandardBooleanColumnSqlAdapter(domain.BooleanColumnSqlAdapter):
    def __init__(
        self, *, col: domain.BooleanColumn, wrapper: typing.Callable[[str], str]
    ):
        super().__init__(col=col, wrapper=wrapper)

    @property
    def definition(self) -> str:
        return f"{self.wrapped_column_name} INT {self.nullable}"

    def literal(self, value: bool) -> str:
        return "1" if value else "0"
