from __future__ import annotations

import abc
import pathlib
import types
import typing

from py_db_adapter import domain
from py_db_adapter.adapter import db_connection, sql_adapter

__all__ = ("DbAdapter",)


logger = domain.logger.getChild("DbAdapter")


class DbAdapter(abc.ABC):
    """Intersection of DbConnection and SqlAdapter"""

    # DBADAPTERS MUST IMPLEMENT THESE METHODS
    @property
    @abc.abstractmethod
    def _connection(self) -> db_connection.DbConnection:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def _sql_adapter(self) -> sql_adapter.SqlAdapter:
        raise NotImplementedError

    @abc.abstractmethod
    def table_exists(
        self, *, table_name: str, schema_name: typing.Optional[str] = None
    ) -> bool:
        raise NotImplementedError

    # PUBLIC INTERFACE
    def add_rows(
        self,
        *,
        schema_name: typing.Optional[str],
        table_name: str,
        rows: domain.Rows,
        batch_size: int,
    ) -> None:
        for batch in rows.batches(batch_size):
            sql = self._sql_adapter.add_rows(
                schema_name=schema_name,
                table_name=table_name,
                parameter_placeholder=self._connection.parameter_placeholder,
                rows=batch,
            )
            params = batch.as_dicts()
            self._connection.execute(sql=sql, params=params)

    def close(self) -> None:
        self._connection.close()

    def commit(self) -> None:
        return self._connection.commit()

    def create_table(self, /, table: domain.Table) -> bool:
        if self.table_exists(
            table_name=table.table_name, schema_name=table.schema_name
        ):
            return True
        else:
            sql = self._sql_adapter.definition(table)
            self._connection.execute(sql=sql)
            logger.info(f"{table.schema_name}.{table.table_name} was created.")
            return False

    def delete_rows(
        self,
        *,
        table: domain.Table,
        rows: domain.Rows,
        batch_size: int,
    ) -> None:
        sql = self._sql_adapter.delete(
            schema_name=table.schema_name,
            table_name=table.table_name,
            pk_cols=table.pk_cols,
            parameter_placeholder=self._connection.parameter_placeholder,
            row_cols=rows.column_names,
        )
        for batch in rows.batches(batch_size):
            params = batch.as_dicts()
            self._connection.execute(sql=sql, params=params)

    def drop_table(
        self, *, table_name: str, schema_name: typing.Optional[str] = None
    ) -> bool:
        if self.table_exists(table_name=table_name, schema_name=schema_name):
            sql = self._sql_adapter.drop(schema_name=schema_name, table_name=table_name)
            self._connection.execute(sql=sql, params=None)
            logger.info(f"{schema_name}.{table_name} was dropped.")
            return True
        else:
            return False

    def fast_row_count(
        self, *, table_name: str, schema_name: typing.Optional[str] = None
    ) -> int:
        if schema_name is None:
            raise domain.exceptions.SchemaIsRequired(
                f"A schema is required for PostgresPyodbcDbAdapter's fast_row_count method"
            )

        sql = self._sql_adapter.fast_row_count(
            schema_name=schema_name, table_name=table_name
        )
        with self._connection as con:
            result = con.fetch(sql=sql, params=None)
            assert result is not None
            if result.is_empty:
                raise domain.exceptions.TableDoesNotExist(
                    table_name=table_name, schema_name=schema_name
                )

            row_ct = result.first_value()
            if not row_ct:
                return self.row_count(schema_name=schema_name, table_name=table_name)

        return typing.cast(int, row_ct)

    def fetch_rows_by_primary_key(
        self,
        *,
        table: domain.Table,
        rows: domain.Rows,
        cols: typing.Optional[typing.Set[str]] = None,
        batch_size: int,
    ) -> domain.Rows:
        pk_cols = {col for col in table.columns if col.column_name in table.pk_cols}
        batches: typing.List[domain.Rows] = []
        for batch in rows.batches(batch_size):
            sql = self._sql_adapter.fetch_rows_by_primary_key_values(
                schema_name=table.schema_name,
                table_name=table.table_name,
                rows=batch,
                pk_cols=pk_cols,
                select_cols=cols,
            )
            if len(pk_cols) == 1:  # where-clause uses IN (...)
                row_batch = self._connection.fetch(sql=sql, params=None)
            else:
                pks = batch.subset(table.pk_cols).as_dicts()
                row_batch = self._connection.fetch(sql=sql, params=pks)
            if row_batch:
                batches.append(row_batch)
        return domain.Rows.concat(batches)

    def inspect_table(
        self,
        *,
        table_name: str,
        schema_name: typing.Optional[str] = None,
        pk_cols: typing.Optional[typing.Set[str]] = None,
        cache_dir: typing.Optional[pathlib.Path] = None,
        sync_cols: typing.Optional[typing.Set[str]] = None,
    ) -> domain.Table:
        return self._connection.inspect_table(
            table_name=table_name,
            schema_name=schema_name,
            pk_cols=pk_cols,
            cache_dir=cache_dir,
            sync_cols=sync_cols,
        )

    def open(self) -> None:
        self._connection.open()

    def row_count(
        self, *, table_name: str, schema_name: typing.Optional[str] = None
    ) -> int:
        sql = self._sql_adapter.row_count(
            schema_name=schema_name, table_name=table_name
        )
        return typing.cast(int, self._connection.fetch(sql=sql).first_value())

    def select_all(
        self,
        *,
        table: domain.Table,
        columns: typing.Optional[typing.Set[str]] = None,
    ) -> domain.Rows:
        sql = self._sql_adapter.select_all(
            schema_name=table.schema_name,
            table_name=table.table_name,
            columns=columns,
        )
        result = self._connection.fetch(sql=sql)
        if result is None:
            return domain.Rows(
                column_names=columns or sorted(table.column_names),
                rows=[],
            )
        else:
            return result

    def table_keys(
        self,
        *,
        table: domain.Table,
        compare_cols: typing.Optional[typing.Set[str]],
    ) -> domain.Rows:
        if compare_cols:
            sql = self._sql_adapter.select_keys(
                schema_name=table.schema_name,
                table_name=table.table_name,
                pk_cols=table.pk_cols,
                change_tracking_cols=compare_cols,
                include_change_tracking_cols=True,
            )
        else:
            sql = self._sql_adapter.select_keys(
                schema_name=table.schema_name,
                table_name=table.table_name,
                pk_cols=table.pk_cols,
                change_tracking_cols=set(),
                include_change_tracking_cols=False,
            )
        result = self._connection.fetch(sql=sql)
        if result is None:
            return domain.Rows(
                column_names=sorted(table.pk_cols | set(compare_cols)),
                rows=[],
            )
        else:
            return result

    def truncate_table(
        self, *, schema_name: typing.Optional[str], table_name: str
    ) -> None:
        sql = self._sql_adapter.truncate(schema_name=schema_name, table_name=table_name)
        self._connection.execute(sql=sql, params=None)

    def update_table(
        self,
        *,
        table: domain.Table,
        rows: domain.Rows,
        batch_size: int,
    ) -> None:
        for batch in rows.batches(batch_size):
            sql = self._sql_adapter.update(
                schema_name=table.schema_name,
                table_name=table.table_name,
                pk_cols=table.pk_cols,
                column_names=table.column_names,
                parameter_placeholder=self._connection.parameter_placeholder,
            )
            pk_cols = sorted(table.pk_cols)
            non_pk_cols = sorted(
                col for col in table.column_names if col not in table.pk_cols
            )
            unordered_params = batch.as_dicts()
            param_order = non_pk_cols + pk_cols
            ordered_params = [
                {k: row[k] for k in param_order} for row in unordered_params
            ]
            self._connection.execute(sql=sql, params=ordered_params)

    # DUNDER METHODS
    def __enter__(self) -> DbAdapter:
        self.open()
        return self

    def __exit__(
        self,
        exc_type: typing.Optional[typing.Type[BaseException]],
        exc_inst: typing.Optional[BaseException],
        exc_tb: typing.Optional[types.TracebackType],
    ) -> typing.Literal[False]:
        self.close()
        return False
