import abc

import typing

from py_db_adapter import domain
from py_db_adapter.adapter.sql_adapter.sql_column_adapter import column_sql_adapter


__all__ = (
    "TextColumnSqlAdapter",
    "StandardTextColumnSqlAdapter",
)


class TextColumnSqlAdapter(
    column_sql_adapter.ColumnSqlAdapter[typing.Type[str]], abc.ABC
):
    def __init__(
        self,
        *,
        column: domain.TextColumn,
        wrapper: typing.Callable[[str], str],
    ):
        super().__init__(column=column, wrapper=wrapper)

    @property
    def column_metadata(self) -> domain.TextColumn:
        return typing.cast(domain.TextColumn, super().column_metadata)

    @abc.abstractmethod
    def literal(self, value: str) -> str:
        raise NotImplementedError


class StandardTextColumnSqlAdapter(TextColumnSqlAdapter):
    def __init__(
        self,
        *,
        column: domain.TextColumn,
        wrapper: typing.Callable[[str], str],
    ):
        super().__init__(column=column, wrapper=wrapper)

    @property
    def definition(self) -> str:
        if max_len := self.column_metadata.max_length:
            return f"{self.wrapped_column_name} VARCHAR({max_len}) {self.nullable}"
        else:
            return f"{self.wrapped_column_name} TEXT {self.nullable}"

    def literal(self, value: str) -> str:
        return str(value)
