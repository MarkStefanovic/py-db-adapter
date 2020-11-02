import abc
import typing

from py_db_adapter import domain
from py_db_adapter.adapter.sql_adapter.sql_column_adapter import column_sql_adapter

__all__ = ("BooleanColumnSqlAdapter", "StandardBooleanColumnSqlAdapter",)


class BooleanColumnSqlAdapter(
    column_sql_adapter.ColumnSqlAdapter[typing.Type[bool]], abc.ABC
):
    def __init__(self, *, column: domain.BooleanColumn, wrapper: typing.Callable[[str], str],):
        super().__init__(column=column, wrapper=wrapper)

    @property
    def column_metadata(self) -> domain.BooleanColumn:
        return typing.cast(domain.BooleanColumn, super().column_metadata)

    @abc.abstractmethod
    def literal(self, value: bool) -> str:
        raise NotImplementedError


class StandardBooleanColumnSqlAdapter(BooleanColumnSqlAdapter):
    def __init__(
        self, *, column: domain.BooleanColumn, wrapper: typing.Callable[[str], str]
    ):
        super().__init__(column=column, wrapper=wrapper)

    @property
    def definition(self) -> str:
        return f"{self.wrapped_column_name} INT {self.nullable}"

    def literal(self, value: bool) -> str:
        return "1" if value else "0"
