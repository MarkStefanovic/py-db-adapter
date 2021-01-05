from __future__ import annotations

import abc
import datetime
import decimal
import typing

import pydantic
import sqlalchemy as sa

from py_db_adapter import domain

__all__ = (
    "Column",
    "BooleanColumn",
    "DateColumn",
    "DateTimeColumn",
    "DecimalColumn",
    "FloatColumn",
    "IntegerColumn",
    "TextColumn",
)


class Column(pydantic.BaseModel, abc.ABC):
    column_name: str
    nullable: bool
    autoincrement: bool

    class Config:
        allow_mutation = False
        anystr_strip_whitespace = True
        min_anystr_length = 1

    @property
    @abc.abstractmethod
    def data_type(self) -> domain.DataType:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def python_data_type(self) -> type:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def sqlalchemy_data_type(self) -> sa.types.TypeEngine:
        raise NotImplementedError

    def to_sqlalchemy_column(self) -> sa.Column:
        return sa.Column(
            self.column_name,
            self.sqlalchemy_data_type,
            nullable=self.nullable,
        )

    def __eq__(self, other: typing.Any) -> bool:
        if other.__class__ is self.__class__:
            other = typing.cast(Column, other)
            return (
                self.column_name,
                self.nullable,
                self.autoincrement,
                self.data_type,
            ) == (
                other.column_name,
                other.nullable,
                other.autoincrement,
                other.data_type,
            )

        else:
            return NotImplemented

    def __hash__(self) -> int:
        return hash(
            (
                self.column_name,
                self.nullable,
                self.autoincrement,
                self.data_type,
            )
        )


class BooleanColumn(Column):
    autoincrement = False

    @property
    def python_data_type(self) -> typing.Type[bool]:
        return bool

    @property
    def sqlalchemy_data_type(self) -> sa.Boolean:
        return sa.Boolean()

    @property
    def data_type(self) -> typing.Literal[domain.DataType.Bool]:
        return domain.DataType.Bool


class DateColumn(Column):
    autoincrement = False

    @property
    def python_data_type(self) -> typing.Type[datetime.date]:
        return datetime.date

    @property
    def sqlalchemy_data_type(self) -> sa.Date:
        return sa.Date()

    @property
    def data_type(self) -> typing.Literal[domain.DataType.Date]:
        return domain.DataType.Date


class DateTimeColumn(Column):
    autoincrement = False

    @property
    def python_data_type(self) -> typing.Type[datetime.datetime]:
        return datetime.datetime

    @property
    def sqlalchemy_data_type(self) -> sa.DateTime:
        return sa.DateTime()

    @property
    def data_type(self) -> typing.Literal[domain.DataType.DateTime]:
        return domain.DataType.DateTime


class DecimalColumn(Column):
    autoincrement = False
    precision: int
    scale: int

    @property
    def data_type(self) -> typing.Literal[domain.DataType.Decimal]:
        return domain.DataType.Decimal

    @property
    def python_data_type(self) -> typing.Type[decimal.Decimal]:
        return decimal.Decimal

    @property
    def sqlalchemy_data_type(self) -> sa.Numeric:
        return sa.Numeric(precision=self.precision, scale=self.scale)


class FloatColumn(Column):
    autoincrement = False

    @property
    def data_type(self) -> typing.Literal[domain.DataType.Float]:
        return domain.DataType.Float

    @property
    def python_data_type(self) -> typing.Type[float]:
        return float

    @property
    def sqlalchemy_data_type(self) -> sa.Float:
        return sa.Float()


class IntegerColumn(Column):
    @property
    def data_type(self) -> typing.Literal[domain.DataType.Int]:
        return domain.DataType.Int

    @property
    def python_data_type(self) -> typing.Type[int]:
        return bool

    @property
    def sqlalchemy_data_type(self) -> sa.types.TypeEngine:
        return sa.BigInteger()


class TextColumn(Column):
    autoincrement = False
    max_length: typing.Optional[int]

    @property
    def data_type(self) -> typing.Literal[domain.DataType.Text]:
        return domain.DataType.Text

    @property
    def python_data_type(self) -> typing.Type[str]:
        return str

    @property
    def sqlalchemy_data_type(self) -> typing.Union[sa.VARCHAR, sa.Text]:
        if self.max_length:
            return sa.VARCHAR(length=self.max_length)
        else:
            return sa.Text()
