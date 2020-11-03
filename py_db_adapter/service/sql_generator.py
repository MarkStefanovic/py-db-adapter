import typing

from py_db_adapter import adapter

__all__ = ("get_keys",)


def delete_rows(
    *,
    sql_adapter: adapter.SqlTableAdapter,
    pks: typing.List[typing.Dict[str, typing.Any]],
) -> str:
    predicates = [
        " AND ".join(f"{col.wrapped_column_name}")
        for col in sql_adapter.column_sql_adapters
    ]
    where_clause = ""
    return f"DELETE FROM {sql_adapter.full_table_name} WHERE {where_clause}"

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


def insert_dummies(sql_adapter: adapter.SqlTableAdapter, /) -> str:
    col_name_csv = ", ".join(sql_adapter.column_names)
    dummy_csv = ", ".join("?" for _ in sql_adapter.column_names)
    return f"INSERT INTO {sql_adapter.full_table_name} ({col_name_csv}) VALUES ({dummy_csv})"
