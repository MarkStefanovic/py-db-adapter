import typing

from py_db_adapter.domain import column

__all__ = ("Table",)


class Table:
    def __init__(
        self,
        *,
        schema_name: typing.Optional[str],
        table_name: str,
        columns: typing.Iterable[column.Column],
        custom_pk_cols: typing.Optional[typing.Iterable[str]] = None,
    ):
        if not isinstance(schema_name, str):
            raise ValueError(
                f"schema_name must be either a string or None, but got {schema_name!r}."
            )
        if not isinstance(table_name, str):
            raise ValueError(f"table_name must be a string, but got {table_name!r}.")
        if not all(isinstance(col, column.Column) for col in columns):
            raise ValueError(
                f"columns must be a sequence of column.Column, but got {columns!r}."
            )

        if custom_pk_cols:
            cols = []
            for col in columns:
                if col.column_name in custom_pk_cols:
                    col.primary_key = True
                cols.append(col)
            self._columns = set(cols)
        else:
            self._columns = set(columns)

        self._schema_name = schema_name
        self._table_name = table_name

    @property
    def columns(self) -> typing.Set[column.Column]:
        return self._columns

    @property
    def column_names(self) -> typing.Set[str]:
        return {col.column_name for col in self._columns}

    @property
    def primary_key_column_names(self) -> typing.Set[str]:
        return {col.column_name for col in self._columns if col.primary_key}

    @property
    def schema_name(self) -> str:
        return self._schema_name

    @property
    def table_name(self) -> str:
        return self._table_name

    def __eq__(self, other: typing.Any) -> bool:
        if other.__class__ is self.__class__:
            other = typing.cast(Table, other)
            return (self._schema_name, self._table_name, self._columns) == (
                other._schema_name,
                other._table_name,
                other._columns,
            )
        else:
            return NotImplemented

    def __hash__(self) -> int:
        return hash((self._schema_name, self._table_name, self._columns))

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}: {self._schema_name}.{self._table_name}>"
