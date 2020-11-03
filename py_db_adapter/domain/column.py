from __future__ import annotations

import abc
import datetime
import decimal
import typing

import sqlalchemy as sa

from py_db_adapter import domain
from py_db_adapter.domain import data_types

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


class Column(abc.ABC):
    def __init__(
        self,
        *,
        schema_name: typing.Optional[str],
        table_name: str,
        column_name: str,
        nullable: bool,
        autoincrement: bool,
        primary_key: bool,
    ):
        self._schema_name = schema_name
        self._table_name = table_name
        self._column_name = column_name
        self._nullable = nullable
        self._autoincrement = autoincrement
        self._primary_key = primary_key

    @property
    def autoincrement(self) -> bool:
        return self._autoincrement

    @property
    def column_name(self) -> str:
        return self._column_name

    @property
    @abc.abstractmethod
    def data_type(self) -> data_types.DataType:
        raise NotImplementedError

    @property
    def nullable(self) -> bool:
        return self._nullable

    @property
    def primary_key(self) -> bool:
        return self._primary_key

    @primary_key.setter
    def primary_key(self, is_primary_key: bool) -> None:
        """A table does not have a primary key and we have to assign it one after the fact"""
        self._primary_key = is_primary_key

    @property
    @abc.abstractmethod
    def python_data_type(self) -> type:
        raise NotImplementedError

    @property
    def schema_name(self) -> typing.Optional[str]:
        return self._schema_name

    @property
    @abc.abstractmethod
    def sqlalchemy_data_type(self) -> sa.types.TypeEngine:
        raise NotImplementedError

    @property
    def table_name(self) -> str:
        return self._table_name

    def to_sqlalchemy_column(self) -> sa.Column:
        return sa.Column(
            self._column_name,
            self.sqlalchemy_data_type,
            primary_key=self._primary_key,
            nullable=self._nullable,
        )

    def __eq__(self, other: typing.Any) -> bool:
        if other.__class__ is self.__class__:
            other = typing.cast(Column, other)
            return (
                self._schema_name,
                self._table_name,
                self._column_name,
                self._nullable,
                self._autoincrement,
                self.data_type,
            ) == (
                other._schema_name,
                other._table_name,
                other._column_name,
                other._nullable,
                other._autoincrement,
                other.data_type,
            )

        else:
            return NotImplemented

    def __hash__(self) -> int:
        return hash(
            (
                self._schema_name,
                self._table_name,
                self._column_name,
                self._nullable,
                self._autoincrement,
                self.data_type,
            )
        )

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(schema_name={self._schema_name}, "
            f"table_name={self._table_name}, column_name={self._column_name}, "
            f"nullable={self._nullable}, autoincrement={self._autoincrement}, "
            f"primary_key={self._primary_key})"
        )


class BooleanColumn(Column):
    def __init__(
        self,
        *,
        schema_name: typing.Optional[str],
        table_name: str,
        column_name: str,
        nullable: bool,
        primary_key: bool,
    ):
        super().__init__(
            schema_name=schema_name,
            table_name=table_name,
            column_name=column_name,
            nullable=nullable,
            autoincrement=False,
            primary_key=primary_key,
        )

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
    def __init__(
        self,
        *,
        schema_name: typing.Optional[str],
        table_name: str,
        column_name: str,
        nullable: bool,
        primary_key: bool,
    ):
        super().__init__(
            schema_name=schema_name,
            table_name=table_name,
            column_name=column_name,
            nullable=nullable,
            autoincrement=False,
            primary_key=primary_key,
        )

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
    def __init__(
        self,
        *,
        schema_name: typing.Optional[str],
        table_name: str,
        column_name: str,
        nullable: bool,
        primary_key: bool,
    ):
        super().__init__(
            schema_name=schema_name,
            table_name=table_name,
            column_name=column_name,
            nullable=nullable,
            autoincrement=False,
            primary_key=primary_key,
        )

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
    def __init__(
        self,
        *,
        schema_name: typing.Optional[str],
        table_name: str,
        column_name: str,
        nullable: bool,
        primary_key: bool,
        precision: int,
        scale: int,
    ):
        self._precision = precision
        self._scale = scale

        super().__init__(
            schema_name=schema_name,
            table_name=table_name,
            column_name=column_name,
            nullable=nullable,
            autoincrement=False,
            primary_key=primary_key,
        )

    @property
    def data_type(self) -> typing.Literal[domain.DataType.Decimal]:
        return domain.DataType.Decimal

    @property
    def precision(self) -> int:
        return self._precision

    @property
    def scale(self) -> int:
        return self._scale

    @property
    def python_data_type(self) -> typing.Type[decimal.Decimal]:
        return decimal.Decimal

    @property
    def sqlalchemy_data_type(self) -> sa.Numeric:
        return sa.Numeric(precision=self.precision, scale=self.scale)


class FloatColumn(Column):
    def __init__(
        self,
        *,
        schema_name: typing.Optional[str],
        table_name: str,
        column_name: str,
        nullable: bool,
        primary_key: bool,
    ):
        super().__init__(
            schema_name=schema_name,
            table_name=table_name,
            column_name=column_name,
            nullable=nullable,
            autoincrement=False,
            primary_key=primary_key,
        )

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
    def __init__(
        self,
        *,
        autoincrement: bool,
        schema_name: typing.Optional[str],
        table_name: str,
        column_name: str,
        nullable: bool,
        primary_key: bool,
    ):
        super().__init__(
            schema_name=schema_name,
            table_name=table_name,
            column_name=column_name,
            nullable=nullable,
            autoincrement=autoincrement,
            primary_key=primary_key,
        )

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
    def __init__(
        self,
        *,
        schema_name: typing.Optional[str],
        table_name: str,
        column_name: str,
        nullable: bool,
        primary_key: bool,
        max_length: typing.Optional[int],
    ):
        self._max_length = max_length

        super().__init__(
            schema_name=schema_name,
            table_name=table_name,
            column_name=column_name,
            nullable=nullable,
            autoincrement=False,
            primary_key=primary_key,
        )

    @property
    def data_type(self) -> typing.Literal[domain.DataType.Text]:
        return domain.DataType.Text

    @property
    def max_length(self) -> typing.Optional[int]:
        return self._max_length

    @property
    def python_data_type(self) -> typing.Type[str]:
        return str

    @property
    def sqlalchemy_data_type(self) -> typing.Union[sa.VARCHAR, sa.Text]:
        if self.max_length:
            return sa.VARCHAR(length=self.max_length)
        else:
            return sa.Text()
