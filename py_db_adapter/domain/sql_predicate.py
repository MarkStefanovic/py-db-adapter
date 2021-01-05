import pydantic
from pydantic import typing

from py_db_adapter.domain import sql_operator


class SqlPredicate(pydantic.BaseModel):
    column_name: str
    operator: sql_operator.SqlOperator
    value: typing.Any

    class Config:
        allow_mutation = False
        anystr_strip_whitespace = True
        arbitrary_types_allowed = True  # needed to handle adapter.DbAdapter
        min_anystr_length = 1