from __future__ import annotations

import abc
import typing

import pyodbc

from py_db_adapter.domain import (
    exceptions,
    logger as domain_logger,
    rows as domain_rows,
    sql_adapter,
    sql_formatter,
    sql_predicate,
    table as domain_table,
)

__all__ = ("DbAdapter",)


logger = domain_logger.root.getChild("DbAdapter")


class DbAdapter(abc.ABC):
    """Intersection of DbConnection and SqlAdapter"""

    @property
    @abc.abstractmethod
    def _sql_adapter(self) -> sql_adapter.SqlAdapter:
        raise NotImplementedError

    @abc.abstractmethod
    def table_exists(
        self,
        *,
        cur: pyodbc.Cursor,
        table_name: str,
        schema_name: typing.Optional[str] = None,
    ) -> bool:
        sql = self._sql_adapter.table_exists(
            schema_name=schema_name, table_name=table_name
        )
        result = cur.execute(sql).fetchval()
        return True if result else False

    def add_rows(
        self,
        *,
        cur: pyodbc.Cursor,
        schema_name: typing.Optional[str],
        table_name: str,
        rows: domain_rows.Rows,
        batch_size: int,
    ) -> None:
        for batch in rows.batches(batch_size):
            sql = self._sql_adapter.add_rows(
                schema_name=schema_name,
                table_name=table_name,
                parameter_placeholder=parameter_placeholder,
                rows=batch,
            )
            params = batch.as_tuples()
            print(f"{sql=}, {params=}")
            cur.executemany(sql, params)

    def create_table(self, *, cur: pyodbc.Cursor, table: domain_table.Table) -> bool:
        if self.table_exists(
            cur=cur, table_name=table.table_name, schema_name=table.schema_name
        ):
            return True
        else:
            sql = self._sql_adapter.definition(table)
            cur.execute(sql)
            logger.info(f"{table.schema_name}.{table.table_name} was created.")
            return False

    def delete_rows(
        self,
        *,
        cur: pyodbc.Cursor,
        table: domain_table.Table,
        rows: domain_rows.Rows,
        batch_size: int,
    ) -> None:
        sql = self._sql_adapter.delete(
            schema_name=table.schema_name,
            table_name=table.table_name,
            pk_cols=set(table.primary_key.columns),
            parameter_placeholder=parameter_placeholder,
            row_cols=rows.column_names,
        )
        for batch in rows.batches(batch_size):
            cur.executemany(sql, batch.as_tuples(sort_columns=False))

    def drop_table(
        self,
        *,
        cur: pyodbc.Cursor,
        table_name: str,
        schema_name: typing.Optional[str] = None,
    ) -> bool:
        if self.table_exists(cur=cur, table_name=table_name, schema_name=schema_name):
            sql = self._sql_adapter.drop(schema_name=schema_name, table_name=table_name)
            cur.execute(sql=sql, params=None)
            logger.info(f"{schema_name}.{table_name} was dropped.")
            return True
        else:
            return False

    def fast_row_count(
        self,
        *,
        cur: pyodbc.Cursor,
        table_name: str,
        schema_name: typing.Optional[str] = None,
    ) -> int:
        if schema_name is None:
            raise exceptions.SchemaIsRequired(
                f"A schema is required for PostgresPyodbcDbAdapter's fast_row_count method"
            )
        sql = self._sql_adapter.fast_row_count(
            schema_name=schema_name, table_name=table_name
        )
        fast_row_count = cur.execute(sql).fetchval()
        if fast_row_count is None:
            return self.row_count(
                cur=cur, table_name=table_name, schema_name=schema_name
            )
        else:
            return fast_row_count

    def fetch_rows_by_primary_key(
        self,
        *,
        cur: pyodbc.Cursor,
        table: domain_table.Table,
        rows: domain_rows.Rows,
        cols: typing.Optional[typing.Set[str]] = None,
        batch_size: int,
    ) -> domain_rows.Rows:
        pk_cols = {
            col for col in table.columns if col.column_name in table.primary_key.columns
        }
        batches: typing.List[domain_rows.Rows] = []
        for batch in rows.batches(batch_size):
            sql = self._sql_adapter.fetch_rows_by_primary_key_values(
                schema_name=table.schema_name,
                table_name=table.table_name,
                rows=batch,
                pk_cols=pk_cols,
                select_cols=cols,
            )
            row_batch = fetch_rows(cur=cur, sql=sql, params=None)
            batches.append(row_batch)
        return domain_rows.Rows.concat(batches)

    def row_count(
        self,
        *,
        cur: pyodbc.Cursor,
        table_name: str,
        schema_name: typing.Optional[str] = None,
    ) -> int:
        sql = self._sql_adapter.row_count(
            schema_name=schema_name, table_name=table_name
        )
        return cur.execute(sql).fetchval()

    def select_all(
        self,
        *,
        cur: pyodbc.Cursor,
        table: domain_table.Table,
        columns: typing.Optional[typing.Set[str]] = None,
    ) -> domain_rows.Rows:
        sql = self._sql_adapter.select_all(
            schema_name=table.schema_name,
            table_name=table.table_name,
            columns=columns,
        )
        return fetch_rows(cur=cur, sql=sql, params=None)

    def select_where(
        self,
        *,
        cur: pyodbc.Cursor,
        table: domain_table.Table,
        predicate: sql_predicate.SqlPredicate,
    ) -> domain_rows.Rows:
        sql = self._sql_adapter.select_where(table=table, predicate=predicate)
        return fetch_rows(cur=cur, sql=sql, params=None)

    def table_keys(
        self,
        *,
        cur: pyodbc.Cursor,
        table: domain_table.Table,
        additional_cols: typing.Optional[typing.Set[str]],
    ) -> domain_rows.Rows:
        cols = (
            set(table.primary_key.columns) | additional_cols
            if additional_cols
            else set(table.primary_key.columns)
        )
        sql = self._sql_adapter.select_distinct(
            schema_name=table.schema_name,
            table_name=table.table_name,
            columns=cols,
        )
        result = fetch_rows(cur=cur, sql=sql, params=None)
        return result.subset(
            column_names=(set(table.primary_key.columns) | set(additional_cols or []))
        )

    def truncate_table(
        self, *, cur: pyodbc.Cursor, schema_name: typing.Optional[str], table_name: str
    ) -> None:
        sql = self._sql_adapter.truncate(schema_name=schema_name, table_name=table_name)
        cur.execute(sql=sql)

    def update_table(
        self,
        *,
        cur: pyodbc.Cursor,
        table: domain_table.Table,
        rows: domain_rows.Rows,
        batch_size: int,
    ) -> None:
        for batch in rows.batches(batch_size):
            sql = self._sql_adapter.update(
                schema_name=table.schema_name,
                table_name=table.table_name,
                pk_cols=set(table.primary_key.columns),
                column_names=table.column_names,
                parameter_placeholder=parameter_placeholder,
            )
            pk_cols = sorted(set(table.primary_key.columns))
            non_pk_cols = sorted(
                col
                for col in table.column_names
                if col not in table.primary_key.columns
            )
            unordered_params = batch.as_dicts()
            param_order = non_pk_cols + pk_cols
            ordered_params = [
                tuple(row[k] for k in param_order) for row in unordered_params
            ]
            cur.executemany(sql, ordered_params)


def fetch_rows(
    *,
    cur: pyodbc.Cursor,
    sql: str,
    params: typing.Optional[typing.List[typing.Tuple[typing.Any, ...]]] = None,
) -> domain_rows.Rows:
    std_sql = sql_formatter.standardize_sql(sql)
    logger.debug(f"FETCH:\n\t{std_sql}\n\tparams={params}")
    if params is None:
        result = cur.execute(std_sql)
    elif len(params) > 1:
        result = cur.executemany(std_sql, params)
    else:
        result = cur.execute(std_sql, params[0])

    column_names = [description[0] for description in cur.description]
    if rows := result.fetchall():
        return domain_rows.Rows(
            column_names=column_names, rows=[tuple(row) for row in rows]
        )
    else:
        return domain_rows.Rows(column_names=column_names, rows=[])


def parameter_placeholder(column_name: str, /) -> str:
    return "?"
