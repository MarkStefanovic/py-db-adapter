from __future__ import annotations

import typing

import pydantic

from py_db_adapter.domain import column

__all__ = ("Table",)


class Table(pydantic.BaseModel):
    schema_name: typing.Optional[str]
    table_name: str
    columns: typing.Set[column.Column]
    pk_cols: typing.Set[str]

    class Config:
        allow_mutation = False
        anystr_strip_whitespace = True
        min_anystr_length = 1

    def add_column(self, /, col: column.Column) -> Table:
        columns = self.columns | {col}
        return self.copy(update={"columns": columns})

    def as_history_table(self) -> Table:
        """Add columns used for row versioning"""
        valid_from_col = column.DateTimeColumn(
            schema_name=self.schema_name,
            table_name=self.table_name,
            column_name="valid_from",
            nullable=False,
            autoincrement=False,
        )
        valid_to_col = column.DateTimeColumn(
            schema_name=self.schema_name,
            table_name=self.table_name,
            column_name="valid_to",
            nullable=False,
            autoincrement=False,
        )
        version_col = column.IntegerColumn(
            schema_name=self.schema_name,
            table_name=self.table_name,
            column_name="version",
            nullable=False,
            autoincrement=False,
        )
        columns = self.columns | {valid_from_col, valid_to_col, version_col}
        hist_table_name = f"{self.table_name}_history"
        return self.copy(update={"table_name": hist_table_name, "columns": columns})

    def column(self, /, column_name: str) -> column.Column:
        return next(col for col in self.columns if col.column_name == column_name)

    @property
    def column_names(self) -> typing.Set[str]:
        return {col.column_name for col in self.columns}

    def copy_table(self, schema_name: str, table_name: str) -> Table:
        return self.copy(update={"schema_name": schema_name, "table_name": table_name})

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
