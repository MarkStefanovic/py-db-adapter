from __future__ import annotations

import typing

import pydantic

from py_db_adapter.domain import column, exceptions

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

    @pydantic.validator("pk_cols")
    def pk_cols_required(cls, v: typing.Set[str]) -> typing.Set[str]:
        if len(v) == 0:
            raise ValueError("pk_cols is required, but none were provided.")
        return v

    @pydantic.validator("columns")
    def columns_required(cls, v: typing.Set[column.Column]) -> typing.Set[column.Column]:
        if len(v) == 0:
            raise ValueError("A table must have at least one column.")
        return v

    def add_column(self, /, col: column.Column) -> Table:
        columns = self.columns | {col}
        return self.copy(update={"columns": columns})

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
            autoincrement=False,
        )
        valid_to_col = column.DateTimeColumn(
            column_name="valid_to",
            nullable=False,
            autoincrement=False,
        )
        columns = self.columns | {id_col, valid_from_col, valid_to_col}
        pk_cols = self.pk_cols | {id_col.column_name}
        return self.copy(
            update={"pk_cols": pk_cols, "table_name": f"{self.table_name}_history", "columns": columns}
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
        return self.copy(update={"schema_name": schema_name, "table_name": table_name})

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
