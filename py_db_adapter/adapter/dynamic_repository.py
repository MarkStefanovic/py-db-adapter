import abc
import typing

import pyodbc

from py_db_adapter import domain, adapter

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
    def keys(self) -> domain.Rows:
        raise NotImplementedError

    @abc.abstractmethod
    def fetch_rows_by_primary_key_values(
        self, *, rows: domain.Rows, columns: typing.Optional[typing.Set[str]]
    ) -> domain.Rows:
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

    def add(self, /, rows: domain.Rows) -> None:
        col_name_csv = ", ".join(
            self._sql_adapter.wrap(col_name) for col_name in rows.column_names
        )
        dummy_csv = ", ".join("?" for _ in rows.column_names)
        sql = f"INSERT INTO {self._sql_adapter.full_table_name} ({col_name_csv}) VALUES ({dummy_csv})"
        with self._connection.cursor() as cur:
            cur.fast_executemany = self._fast_executemany
            params = rows.as_tuples()
            print(f"Executing SQL:\n\t{sql}\n\t{params=}")
            cur.executemany(sql, params)

    def delete(self, /, rows: domain.Rows) -> None:
        pk_col_names = {
            col.column_metadata.column_name
            for col in self._sql_adapter.primary_key_column_sql_adapters
        }
        where_clause = " AND ".join(
            f"{self._sql_adapter.wrap(col)} = ?"
            for col in rows.column_names
            if col in pk_col_names
        )
        sql = f"DELETE FROM {self._sql_adapter.full_table_name} WHERE {where_clause}"
        with self._connection.cursor() as cur:
            cur.fast_executemany = self._fast_executemany
            params = rows.as_tuples()
            print(f"Executing SQL:\n\t{sql}\n\t{params=}")
            cur.executemany(sql, params)

    def fetch_rows_by_primary_key_values(
        self, *, rows: domain.Rows, columns: typing.Optional[typing.Set[str]]
    ) -> domain.Rows:
        pk_col_names = [
            col.column_metadata.column_name
            for col in self._sql_adapter.primary_key_column_sql_adapters
        ]
        if len(pk_col_names) == 1:
            pk_col_name = pk_col_names[0]
            wrapped_pk_col_name = self._sql_adapter.wrap(pk_col_name)
            col_adapter = next(
                col
                for col in self._sql_adapter.column_sql_adapters
                if col.column_metadata.column_name == pk_col_name
            )
            pk_values = rows.column(pk_col_name)
            pk_values_csv = ",".join(col_adapter.literal(v) for v in pk_values)
            where_clause = f"{wrapped_pk_col_name} IN ({pk_values_csv})"
        else:
            wrapped_pk_col_names = {}
            for col_name in rows.column_names:
                if col_name in pk_col_names:
                    wrapped_col_name = self._sql_adapter.wrap(col_name)
                    wrapped_pk_col_names[wrapped_col_name] = rows.column_indices[
                        col_name
                    ]
            predicates = []
            for row in rows.as_tuples():
                predicate = " AND ".join(
                    f"{col_name} = {row[ix]}"
                    for col_name, ix in wrapped_pk_col_names.items()
                )
                predicates.append(predicate)
            where_clause = " OR ".join(f"({predicate})" for predicate in predicates)
        if columns:
            select_col_names = [
                col.wrapped_column_name
                for col in self._sql_adapter.column_sql_adapters
                if col.column_metadata.column_name in columns
            ]
        else:
            select_col_names = [
                col.wrapped_column_name for col in self._sql_adapter.column_sql_adapters
            ]
        select_cols_csv = ", ".join(
            col.wrapped_column_name for col in self._sql_adapter.column_sql_adapters
        )
        sql = f"SELECT {select_cols_csv} FROM {self._sql_adapter.full_table_name} WHERE {where_clause}"
        with self._connection.cursor() as cur:
            print(f"Executing SQL:\n\t{sql}")
            result = cur.execute(sql).fetchall()
            return domain.Rows(
                column_names=select_col_names, rows=[tuple(row) for row in result]
            )

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
        if change_cols_csv:
            select_cols_csv = f"{pk_cols_csv}, {change_cols_csv}"
        else:
            select_cols_csv = pk_cols_csv
        sql = f"SELECT DISTINCT {select_cols_csv} FROM {self._sql_adapter.full_table_name}"
        with self._connection.cursor() as cur:
            print(f"Executing sql:\n\t{sql}")
            result = cur.execute(sql).fetchall()
            column_names = [col[0] for col in cur.description]
            rows = [tuple(row) for row in result]
            return domain.Rows(
                column_names=column_names,
                rows=rows,
            )
            # return [dict(zip(column_names, row)) for row in result]

    def update(self, /, rows: domain.Rows) -> None:
        pk_col_names = {
            col.column_metadata.column_name
            for col in self._sql_adapter.primary_key_column_sql_adapters
            if col.column_metadata.column_name in rows.column_names
        }
        param_indices = []
        non_pk_col_wrapped_names = []
        for col_name in rows.column_names:
            if col_name not in pk_col_names:
                wrapped_name = self._sql_adapter.wrap(col_name)
                non_pk_col_wrapped_names.append(wrapped_name)
                row_col_index = rows.column_indices[col_name]
                param_indices.append(row_col_index)
        set_clause = ", ".join(
            f"{col_name} = ?" for col_name in non_pk_col_wrapped_names
        )
        pk_col_wrapped_names = []
        for col_name in rows.column_names:
            if col_name in pk_col_names:
                wrapped_name = self._sql_adapter.wrap(col_name)
                pk_col_wrapped_names.append(wrapped_name)
                row_col_index = rows.column_indices[col_name]
                param_indices.append(row_col_index)
        where_clause = " AND ".join(
            f"{col_name} = ?" for col_name in pk_col_wrapped_names
        )

        sql = f"UPDATE {self._sql_adapter.full_table_name} SET {set_clause} WHERE {where_clause}"
        params = [tuple(row[i] for i in param_indices) for row in rows.as_tuples()]
        with self._connection.cursor() as cur:
            cur.fast_executemany = self._fast_executemany
            print(f"Executing SQL:\n\tsql:{sql}\n\tparams: {params}")
            cur.executemany(sql, params)

    def upsert_table(
        self,
        *,
        source_repo: DynamicRepository,
        add: bool = True,
        update: bool = True,
        delete: bool = True,
    ) -> None:
        key_cols = {
            col.column_metadata.column_name
            for col in self._sql_adapter.column_sql_adapters
            if col.column_metadata.primary_key
        }
        changes = compare_rows(
            key_cols=key_cols, src_rows=source_repo.keys(), dest_rows=self.keys()
        )
        src_column_names = {
            col.column_metadata.column_name
            for col in source_repo.sql_adapter.column_sql_adapters
        }
        dest_column_names = {
            col.column_metadata.column_name
            for col in self._sql_adapter.column_sql_adapters
        }
        common_cols = src_column_names.intersection(dest_column_names)
        if changes["added"].row_count and add:
            new_rows = source_repo.fetch_rows_by_primary_key_values(
                rows=changes["added"], columns=common_cols
            )
            self.add(new_rows)
        if changes["deleted"].row_count and delete:
            self.delete(changes["deleted"])
        if changes["updated"].row_count and update:
            updated_rows = source_repo.fetch_rows_by_primary_key_values(
                rows=changes["updated"], columns=common_cols
            )
            self.update(updated_rows)


def fetch_rows(con: pyodbc.Connection, sql: str) -> domain.Rows:
    with con.cursor() as cur:
        print(f"Executing SQL:\n\t{sql}")
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
    common_cols = sorted(
        set(src_rows.column_names).intersection(set(dest_rows.column_names))
    )
    compare_cols = {col for col in common_cols if col not in key_cols}
    key_cols = {col for col in key_cols if col in common_cols}
    src_hashes = src_rows.as_lookup_table(
        key_columns=key_cols, value_columns=compare_cols
    )
    dest_hashes = dest_rows.as_lookup_table(
        key_columns=key_cols, value_columns=compare_cols
    )
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
        "added": domain.Rows.from_lookup_table(
            lookup_table=added,
            key_columns=key_cols,
            value_columns=compare_cols,
        ),
        "deleted": domain.Rows.from_lookup_table(
            lookup_table=deleted,
            key_columns=key_cols,
            value_columns=compare_cols,
        ),
        "updated": domain.Rows.from_lookup_table(
            lookup_table=updates,
            key_columns=key_cols,
            value_columns=compare_cols,
        ),
    }
