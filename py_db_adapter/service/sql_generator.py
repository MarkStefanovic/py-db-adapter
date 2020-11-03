import typing

from py_db_adapter import adapter

__all__ = ("get_keys",)


def get_keys(
    *,
    sql_adapter: adapter.SqlTableAdapter,
    additional_cols: typing.Iterable[str],
    pk_cols: typing.Optional[typing.Iterable[str]] = None,
) -> str:
    if pk_cols is None:
        pk_col_names = [
            col.wrapped_column_name
            for col in sql_adapter.column_sql_adapters
            if col.column_metadata.primary_key
        ]
    else:
        pk_col_names = [sql_adapter.wrap(col) for col in pk_cols]

    pk_col_str = ", ".join(pk_col_names)
    additional_cols_str = ", ".join(
        col.wrapped_column_name
        for col in sql_adapter.column_sql_adapters
        if col.column_metadata.column_name in additional_cols
    )
    return (
        f"SELECT {pk_col_str}, {additional_cols_str} FROM {sql_adapter.full_table_name}"
    )
