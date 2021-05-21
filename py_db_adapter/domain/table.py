from __future__ import annotations

import dataclasses
import typing

from py_db_adapter.domain import column, exceptions

__all__ = ("Table",)


@dataclasses.dataclass(frozen=True)
class Table:
    schema_name: typing.Optional[str]
    table_name: str
    columns: typing.Set[column.Column]
    pk_cols: typing.Set[str]

    def __post_init__(self):
        if not self.pk_cols:
            raise exceptions.TableMissingPrimaryKey(
                schema_name=self.schema_name, table_name=self.table_name
            )
        if not self.columns:
            raise exceptions.TableHasNoColumns(
                schema_name=self.schema_name, table_name=self.table_name
            )

    def add_column(self, /, col: column.Column) -> Table:
        columns = self.columns | {col}
        return dataclasses.replace(self, columns=columns)

    def as_history_table(self) -> Table:
        """Add columns used for row versioning"""
        id_col = column.IntegerColumn(
            column_name=f"{self.table_name}_history_id",
            nullable=False,
            autoincrement=True,
        )
        valid_from_col = column.DateTimeColumn(
            column_name="valid_from",
            nullable=False,
        )
        valid_to_col = column.DateTimeColumn(
            column_name="valid_to",
            nullable=False,
        )
        columns = self.columns | {id_col, valid_from_col, valid_to_col}
        pk_cols = self.pk_cols | {id_col.column_name}
        return dataclasses.replace(
            self,
            pk_cols=pk_cols,
            table_name=f"{self.table_name}_history",
            columns=columns,
        )

    def column_by_name(self, /, column_name: str) -> column.Column:
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

    def copy_table(self, schema_name: str, table_name: str) -> Table:
        return dataclasses.replace(
            self,
            schema_name=schema_name,
            table_name=table_name,
        )

    @property
    def non_pk_column_names(self) -> typing.Set[str]:
        return self.column_names - self.pk_cols

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
                frozenset(self.pk_cols),
            )
        )

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}: {self.schema_name}.{self.table_name}>"
