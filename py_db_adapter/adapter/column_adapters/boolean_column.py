import abc
import typing

from py_db_adapter import domain

__all__ = (
    "BooleanColumnSqlAdapter",
    "StandardBooleanColumnSqlAdapter",
)


class BooleanColumnSqlAdapter(domain.ColumnSqlAdapter[bool], abc.ABC):
    def __init__(
        self,
        *,
        column: domain.BooleanColumn,
        wrapper: typing.Callable[[str], str],
    ):
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
