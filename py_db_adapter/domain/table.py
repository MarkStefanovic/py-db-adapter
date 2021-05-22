from __future__ import annotations

import dataclasses
import typing

from py_db_adapter.domain import exceptions
from py_db_adapter.domain.column import Column
from py_db_adapter.domain.data_types import DataType
from py_db_adapter.domain.primary_key import PrimaryKey
from py_db_adapter.domain.unique_constraint import UniqueContraint

__all__ = ("Table",)


@dataclasses.dataclass(frozen=True)
class Table:
    schema_name: typing.Optional[str]
    table_name: str
    columns: typing.FrozenSet[Column]
    primary_key: PrimaryKey
    unique_constraints: typing.FrozenSet[UniqueContraint] = frozenset()

    def __post_init__(self) -> None:
        if not self.columns:
            raise exceptions.TableHasNoColumns(
                schema_name=self.schema_name, table_name=self.table_name
            )

    def add_column(self, /, col: Column) -> Table:
        columns = self.columns | {col}
        return dataclasses.replace(self, columns=columns)

    def as_history_table(self) -> Table:
        """Add columns used for row versioning"""
        id_col = Column(
            column_name=f"{self.table_name}_history_id",
            nullable=False,
            data_type=DataType.Int,
            autoincrement=True,
        )
        valid_from_col = Column(
            column_name="valid_from",
            nullable=False,
            data_type=DataType.DateTime,
        )
        valid_to_col = Column(
            column_name="valid_to",
            nullable=False,
            data_type=DataType.DateTime,
        )
        columns = self.columns | {id_col, valid_from_col, valid_to_col}
        pk_cols = self.primary_key.columns + (id_col.column_name,)
        pk = PrimaryKey(
            schema_name=self.schema_name,
            table_name=self.table_name,
            columns=pk_cols,
        )
        return dataclasses.replace(
            self,
            primary_key=pk,
            table_name=f"{self.table_name}_history",
            columns=columns,
            unique_constraints=frozenset(
                {UniqueContraint(columns=(id_col.column_name, "valid_to"))}
            ),
        )

    def column_by_name(self, /, column_name: str) -> Column:
        try:
            return next(col for col in self.columns if col.column_name == column_name)
        except StopIteration:
            raise exceptions.ColumnNameNotFound(
                column_name=column_name,
                table_name=self.table_name,
                available_cols=self.column_names,
            )

    @property
    def column_names(self) -> typing.Set[str]:
        return {col.column_name for col in self.columns}

    def copy_table(self, schema_name: typing.Optional[str], table_name: str) -> Table:
        return dataclasses.replace(
            self,
            schema_name=schema_name,
            table_name=table_name,
        )

    @property
    def non_pk_column_names(self) -> typing.Set[str]:
        return self.column_names - set(self.primary_key.columns)

    def __eq__(self, other: typing.Any) -> bool:
        if other.__class__ is self.__class__:
            other = typing.cast(Table, other)
            return (self.schema_name, self.table_name) == (
                other.schema_name,
                other.table_name,
            )
        else:
            return NotImplemented

    def __hash__(self) -> int:
        return hash(
            (
                self.schema_name,
                self.table_name,
                frozenset(self.columns),
                frozenset(self.primary_key.columns),
            )
        )

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}: {self.schema_name}.{self.table_name}>"
