import dataclasses
import typing

from py_db_adapter.domain import sql_operator

__all__ = ("SqlPredicate",)


@dataclasses.dataclass(frozen=True)
class SqlPredicate:
    column_name: str
    operator: sql_operator.SqlOperator
    value: typing.Any
