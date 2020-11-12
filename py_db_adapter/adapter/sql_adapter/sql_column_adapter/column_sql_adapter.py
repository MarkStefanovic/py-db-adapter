from __future__ import annotations

import abc
import datetime
import decimal
import typing

from py_db_adapter import domain

__all__ = (
    "ColumnSqlAdapter",
)

D = typing.TypeVar(
    "D",
    bool,
    datetime.date,
    datetime.datetime,
    decimal.Decimal,
    float,
    int,
    str,
)


class ColumnSqlAdapter(abc.ABC, typing.Generic[D]):
    def __init__(self, *, column: domain.Column, wrapper: typing.Callable[[str], str]):
        self._column = column
        self._wrapper = wrapper

    def coalesce(
        self,
        default_value: D,
        table_qualifier: typing.Optional[str] = None,
        column_alias: typing.Optional[str] = None,
    ) -> str:
        default = self.literal(default_value)
        if table_qualifier is None and column_alias is None:
            return (
                f"COALESCE({self.wrapped_column_name}, {default}) AS "
                f"{self.wrapped_column_name}"
            )
        elif table_qualifier is None:
            return f"COALESCE({self.wrapped_column_name}, {default}) AS {column_alias}"
        elif column_alias is None:
            return (
                f"COALESCE({table_qualifier}.{self.wrapped_column_name}, {default}) AS "
                f"{self.wrapped_column_name}"
            )
        else:
            return (
                f"COALESCE({table_qualifier}.{self.wrapped_column_name}, {default}) AS "
                f"{column_alias}"
            )

    @property
    def column_metadata(self) -> domain.Column:
        return self._column

    @property
    @abc.abstractmethod
    def definition(self) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    def literal(self, value: D) -> str:
        raise NotImplementedError

    @property
    def nullable(self) -> str:
        if self._column.nullable:
            return "NULL"
        else:
            return "NOT NULL"

    @property
    def wrapped_column_name(self) -> str:
        return self._wrapper(self._column.column_name)

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}: {self.wrapped_column_name}>"
