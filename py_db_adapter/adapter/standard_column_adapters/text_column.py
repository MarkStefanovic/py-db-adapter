import typing

from py_db_adapter import domain

__all__ = ("StandardTextColumnSqlAdapter",)


class StandardTextColumnSqlAdapter(domain.TextColumnSqlAdapter):
    def __init__(
        self,
        *,
        col: domain.TextColumn,
        wrapper: typing.Callable[[str], str],
    ):
        super().__init__(col=col, wrapper=wrapper)

    @property
    def definition(self) -> str:
        if max_len := self.column_metadata.max_length:
            return f"{self.wrapped_column_name} VARCHAR({max_len}) {self.nullable}"
        else:
            return f"{self.wrapped_column_name} TEXT {self.nullable}"

    def literal(self, value: str) -> str:
        return f"{value!r}"
