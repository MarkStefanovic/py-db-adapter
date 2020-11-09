import abc
import typing

import pyodbc

from py_db_adapter import domain, adapter
import sqlalchemy as sa

__all__ = (
    "DynamicRepository",
    "PyodbcDynamicRepository",
)

def chunk_items(
    items: typing.Collection[typing.Any], n: int
) -> typing.List[typing.Iterable[typing.Any]]:
    items = list(items)
    return [items[i : i + n] for i in range(0, len(items), n)]


class DynamicRepository(abc.ABC):
    def __init__(
        self,
        *,
        sql_adapter: adapter.SqlTableAdapter,
        change_tracking_columns: typing.Optional[typing.Iterable[str]] = None,
    ):
        self._sql_adapter = sql_adapter
        self._change_tracking_columns = sorted(set(change_tracking_columns or []))

    @property
    def change_tracking_columns(self) -> typing.List[str]:
        if self._change_tracking_columns is None:
            return [
                col.column_metadata.column_name
                for col in self._sql_adapter.column_sql_adapters
            ]
        else:
            return self._change_tracking_columns

    @abc.abstractmethod
    def delete(self, /, rows: domain.Rows) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def add(self, /, rows: domain.Rows) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def update(self, /, rows: domain.Rows) -> None:
        raise NotImplementedError

    @property
    def sql_adapter(self) -> adapter.SqlTableAdapter:
        return self._sql_adapter


class PyodbcDynamicRepository(DynamicRepository):
    def __init__(
        self,
        *,
        connection: pyodbc.Connection,
        sql_adapter: adapter.SqlTableAdapter,
        change_tracking_columns: typing.Optional[typing.Iterable[str]] = None,
        fast_executemany: bool = True,
    ):
        super().__init__(
            sql_adapter=sql_adapter,
            change_tracking_columns=change_tracking_columns,
        )

        self._connection = connection
        self._fast_executemany = fast_executemany

    def delete(self, /, rows: domain.Rows) -> None:
        where_clause = " AND ".join(
            f"{self.sql_adapter.wrap(col)} = ?"
            for col in rows.column_names
        )
        sql = f"DELETE FROM {self.sql_adapter.full_table_name} WHERE {where_clause}"
        with self._connection.cursor() as cur:
            cur.fast_executemany = True
            cur.executemany(sql, rows.as_tuples())

    @property
    def keys(self) -> domain.Rows:
        pk_cols_csv = ", ".join(
            col.wrapped_column_name
            for col in self._sql_adapter.primary_key_column_sql_adapters
        )
        change_cols_csv = ", ".join(
            col.wrapped_column_name
            for col in self._sql_adapter.column_sql_adapters
            if col.column_metadata.column_name in self._change_tracking_columns
        )
        sql = f"SELECT {pk_cols_csv}, {change_cols_csv} FROM {self._sql_adapter.full_table_name}"
        with self._connection.cursor() as cur:
            result = cur.execute(sql).fetch_rows()
            column_names = [col[0] for col in cur.description]
            rows = [tuple(row) for row in result]
            return domain.Rows(
                column_names=column_names,
                rows=rows,
            )
            # return [dict(zip(column_names, row)) for row in result]

    def add(self, /, rows: domain.Rows) -> None:
        col_name_csv = ", ".join(
            self._sql_adapter.wrap(col_name) for col_name in rows.column_names
        )
        dummy_csv = ", ".join("?" for _ in rows.column_names)
        sql = (
            f"INSERT INTO {self._sql_adapter.full_table_name} ({col_name_csv}) VALUES ({dummy_csv})"
        )
        with self._connection.cursor() as cur:
            cur.fast_executemany = self._fast_executemany
            cur.executemany(sql, rows.as_tuples())

    def update(self, /, rows: domain.Rows) -> None:
        pass

# def get_keys(
#     *,
#     sql_adapter: adapter.SqlTableAdapter,
#     con_or_engine: typing.Union[pyodbc.Connection, sa.engine.Engine],
#     additional_cols: typing.List[str],
# ) -> typing.List[typing.Dict[str, typing.Any]]:
#     sql = sql_generator.get_keys(
#         sql_adapter=sql_adapter,
#         additional_cols=additional_cols,
#     )
#     if isinstance(con_or_engine, pyodbc.Connection):
#         with con_or_engine.cursor() as cur:
#             result = cur.execute(sql).fetchall()
#             column_names = [col[0] for col in cur.description]
#             return [dict(zip(column_names, row)) for row in result]
#     else:
#         with con_or_engine.begin() as con:
#             result = con.execute(sa.text(sql)).fetchall()
#             return [dict(row) for row in result]


def fetch_rows(con: pyodbc.Connection, sql: str) -> domain.Rows:
    with con.cursor() as cur:
        result = cur.execute(sql).fetchall()
        column_names = [col[0] for col in cur.description]
        rows = [tuple(row) for row in result]
        return domain.Rows(column_names=column_names, rows=rows)


def compare_rows(
    *,
    key_cols: typing.Set[str],
    src_rows: domain.Rows,
    dest_rows: domain.Rows,
) -> typing.Dict[str, domain.Rows]:
    compare_cols = {col for col in src_rows.column_names if col not in key_cols}
    src_hashes = src_rows.as_lookup_table(key_columns=key_cols, value_columns=compare_cols)
    dest_hashes = dest_rows.as_lookup_table(key_columns=key_cols, value_columns=compare_cols)
    src_key_set = set(src_hashes.keys())
    dest_key_set = set(dest_hashes.keys())
    added = {k: src_hashes[k] for k in (src_key_set - dest_key_set)}
    deleted = {k: src_hashes.get(k, tuple()) for k in (dest_key_set - src_key_set)}
    updates = {
        k: src_hashes.get(k, tuple())
        for k in src_key_set
        if k not in added
        and k not in deleted
        and src_hashes.get(k, tuple()) != dest_hashes.get(k, tuple())
    }
    return {
        "added": added,
        "deleted": deleted,
        "updated": updates,
    }


# def get_changes(
#     src_con_or_engine: typing.Union[pyodbc.Connection, sa.engine.Engine],
#     dest_con_or_engine: typing.Union[pyodbc.Connection, sa.engine.Engine],
#     src_sql_adapter: adapter.SqlTableAdapter,
#     dest_sql_adapter: adapter.SqlTableAdapter,
#     compare_cols: typing.List[str],
# ) -> typing.Dict[
#     str,
#     typing.Dict[
#         typing.Tuple[typing.Tuple[str, typing.Any], ...],
#         typing.Tuple[typing.Tuple[str, typing.Any], ...],
#     ],
# ]:
#     pk_cols = src_sql_adapter.table_metadata.primary_key_column_names
#     src_rows = get_keys(
#         sql_adapter=src_sql_adapter,
#         con_or_engine=src_con_or_engine,
#         additional_cols=compare_cols,
#     )
#     dest_rows = get_keys(
#         sql_adapter=dest_sql_adapter,
#         con_or_engine=dest_con_or_engine,
#         additional_cols=compare_cols,
#     )
#     return compare_rows(
#         pk_cols=pk_cols,
#         compare_cols=compare_cols,
#         src_rows=src_rows,
#         dest_rows=dest_rows,
#     )


# def insert_rows(
#     *,
#     sql_adapter: adapter.SqlTableAdapter,
#     con: pyodbc.Connection,
#     rows: typing.List[typing.Dict[str, typing.Any]],
#     fast_executemany: bool = True,
# ) -> int:
#     with con.cursor() as cur:
#         cur.fast_executemany = fast_executemany
#         columns = sorted(rows[0].keys())
#         row_values = [tuple(v for k, v in sorted(row.items())) for row in rows]
#         sql = sql_generator.insert_rows(sql_adapter=sql_adapter, columns=columns)
#         cur.executemany(sql, row_values)
#         return len(rows)
