import abc
import dataclasses
import datetime
import decimal
import typing

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


@dataclasses.dataclass(frozen=True)
class Column(abc.ABC):
    column_name: str
    nullable: bool

    @property
    @abc.abstractmethod
    def data_type(self) -> data_types.DataType:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def python_data_type(self) -> type:
        raise NotImplementedError

    def __eq__(self, other: typing.Any) -> bool:
        if other.__class__ is self.__class__:
            other = typing.cast(Column, other)
            return (self.column_name, self.nullable, self.data_type,) == (
                other.column_name,
                other.nullable,
                other.data_type,
            )

        else:
            return NotImplemented

    def __hash__(self) -> int:
        return hash(
            (
                self.column_name,
                self.nullable,
                self.data_type,
            )
        )


@dataclasses.dataclass(frozen=True)
class BooleanColumn(Column):
    @property
    def python_data_type(self) -> typing.Type[bool]:
        return bool

    @property
    def data_type(self) -> typing.Literal[data_types.DataType.Bool]:
        return data_types.DataType.Bool


@dataclasses.dataclass(frozen=True)
class DateColumn(Column):
    @property
    def python_data_type(self) -> typing.Type[datetime.date]:
        return datetime.date

    @property
    def data_type(self) -> typing.Literal[data_types.DataType.Date]:
        return data_types.DataType.Date


@dataclasses.dataclass(frozen=True)
class DateTimeColumn(Column):
    @property
    def python_data_type(self) -> typing.Type[datetime.datetime]:
        return datetime.datetime

    @property
    def data_type(self) -> typing.Literal[data_types.DataType.DateTime]:
        return data_types.DataType.DateTime


@dataclasses.dataclass(frozen=True)
class DecimalColumn(Column):
    precision: int
    scale: int

    @property
    def data_type(self) -> typing.Literal[data_types.DataType.Decimal]:
        return data_types.DataType.Decimal

    @property
    def python_data_type(self) -> typing.Type[decimal.Decimal]:
        return decimal.Decimal


@dataclasses.dataclass(frozen=True)
class FloatColumn(Column):
    @property
    def data_type(self) -> typing.Literal[data_types.DataType.Float]:
        return data_types.DataType.Float

    @property
    def python_data_type(self) -> typing.Type[float]:
        return float


@dataclasses.dataclass(frozen=True)
class IntegerColumn(Column):
    autoincrement: bool = False

    @property
    def data_type(self) -> typing.Literal[data_types.DataType.Int]:
        return data_types.DataType.Int

    @property
    def python_data_type(self) -> typing.Type[int]:
        return bool


@dataclasses.dataclass(frozen=True)
class TextColumn(Column):
    max_length: typing.Optional[int]

    @property
    def data_type(self) -> typing.Literal[data_types.DataType.Text]:
        return data_types.DataType.Text

    @property
    def python_data_type(self) -> typing.Type[str]:
        return str


if __name__ == "__main__":
    col = TextColumn(
        column_name="first_name",
        nullable=False,
        max_length=10,
    )
    print(f"{col=}")
