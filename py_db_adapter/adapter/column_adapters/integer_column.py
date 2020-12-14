import abc

import typing

from py_db_adapter import domain

__all__ = (
    "IntegerColumnSqlAdapter",
    "StandardIntegerColumnSqlAdapter",
)


class IntegerColumnSqlAdapter(domain.ColumnSqlAdapter[int], abc.ABC):
    def __init__(
        self,
        *,
        column: domain.IntegerColumn,
        wrapper: typing.Callable[[str], str],
    ):
        super().__init__(column=column, wrapper=wrapper)

    @property
    def column_metadata(self) -> domain.Column:
        return typing.cast(domain.IntegerColumn, super().column_metadata)

    @abc.abstractmethod
    def literal(self, value: int) -> str:
        raise NotImplementedError


class StandardIntegerColumnSqlAdapter(IntegerColumnSqlAdapter):
    def __init__(
        self,
        *,
        column: domain.IntegerColumn,
        wrapper: typing.Callable[[str], str],
    ):
        super().__init__(column=column, wrapper=wrapper)

    @property
    def definition(self) -> str:
        return f"{self.wrapped_column_name} BIGINT {self.nullable}"

    def literal(self, value: int) -> str:
        return str(value)
