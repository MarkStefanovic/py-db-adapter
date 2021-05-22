import dataclasses
import typing

from py_db_adapter.domain import data_types

__all__ = ("Column",)


@dataclasses.dataclass(frozen=True)
class Column:
    column_name: str
    nullable: bool
    data_type: data_types.DataType
    autoincrement: bool = False
    precision: typing.Optional[int] = None
    scale: typing.Optional[int] = None
    max_length: typing.Optional[int] = None

    @property
    def python_data_type(self) -> type:
        return self.data_type.py_type

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


if __name__ == "__main__":
    col = Column(
        column_name="first_name",
        nullable=False,
        max_length=10,
        data_type=data_types.DataType.Text,
    )
    print(f"{col=}")
