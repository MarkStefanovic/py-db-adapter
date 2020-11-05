import abc
import enum
import typing

from py_db_adapter import adapter

__all__ = ("get_keys",)


class ExecutorType(enum.Enum):
    PYODBC = "pyodbc"
    SQLALCHEMY = "sqlalchemy"


class SqlGenerator(abc.ABC):
    def __init__(self, *, executor_type: ExecutorType, sql_adapter: adapter.SqlTableAdapter):
        self._executor_type = executor_type
        self._sql_adapter = sql_adapter

    @abc.abstractmethod
    def delete_rows(self) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    def get_keys

    @property
    def executor_type(self) -> ExecutorType:
        return self._executor_type

    @property
    def sql_adapter(self) -> adapter.SqlTableAdapter:
        return self._sql_adapter


def delete_rows_dummy(
    sql_adapter: adapter.SqlTableAdapter,
    /,
) -> str:
    where_clause = " AND ".join(
        f"{col.wrapped_column_name} = ?"
        for col in sql_adapter.primary_key_column_sql_adapters
    )
    return f"DELETE FROM {sql_adapter.full_table_name} WHERE {where_clause}"


def get_keys(
    *,
    sql_adapter: adapter.SqlTableAdapter,
    additional_cols: typing.Iterable[str],
) -> str:
    pk_col_str = ", ".join(
        col.wrapped_column_name for col in sql_adapter.primary_key_column_sql_adapters
    )
    additional_cols_str = ", ".join(
        col.wrapped_column_name
        for col in sql_adapter.column_sql_adapters
        if col.column_metadata.column_name in additional_cols
    )
    return (
        f"SELECT {pk_col_str}, {additional_cols_str} FROM {sql_adapter.full_table_name}"
    )


def insert_rows(
    *,
    sql_adapter: adapter.SqlTableAdapter,
    columns: typing.Optional[typing.Iterable[str]] = None,
) -> str:
    if columns is None:
        column_names = [
            col.wrapped_column_name for col in sql_adapter.column_sql_adapters
        ]
    else:
        column_names = [
            col.wrapped_column_name
            for col in sql_adapter.column_sql_adapters
            if col.column_metadata.column_name in columns
        ]

    col_name_csv = ", ".join(column_names)
    dummy_csv = ", ".join("?" for _ in sql_adapter.column_sql_adapters)
    return f"INSERT INTO {sql_adapter.full_table_name} ({col_name_csv}) VALUES ({dummy_csv})"
