import typing

from py_db_adapter import adapter

__all__ = ("get_keys",)


def get_keys(
    *,
    sql_adapter: adapter.SqlTableAdapter,
    additional_cols: typing.List[str],
) -> typing.List[typing.Dict[str, typing.Any]]:
    pk_col_str = ", ".join(
        col.wrapped_column_name
        for col in sql_adapter.column_sql_adapters
        if col.column_metadata.primary_key
    )
    additional_cols_str = ", ".join(
        col.wrapped_column_name
        for col in sql_adapter.column_sql_adapters
        if col.column_metadata.column_name in additional_cols
    )
    return f"SELECT {pk_col_str}, {additional_cols_str} FROM {sql_adapter.full_table_name}"
