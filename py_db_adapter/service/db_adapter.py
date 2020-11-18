import abc

import typing

from py_db_adapter import adapter


class DbAdapterService(abc.ABC):
    def fast_row_count(
        self, *, table_name: str, schema_name: typing.Optional[str] = None
    ) -> int:
        raise NotImplementedError

    def inspect_table(self):
        raise NotImplementedError

    def table_exists(
        self, *, table_name: str, schema_name: typing.Optional[str] = None
    ) -> bool:
        raise NotImplementedError

    def upsert_table(self):
        raise NotImplementedError


class DbAdapterService(DbAdapterService):
    def __init__(self, *, db: adapter.Db, ):
        self._db = db

    def copy_table(
        *,
        src_table: domain.Table,
        dest_con: adapter.DbConnection,
        dest_schema_name: str,
        dest_table_name: str,
    ) -> None:
        dest_table = src_table.copy(
            new_schema_name=dest_schema_name,
            new_table_name=dest_table_name,
        )
        sql_adapter = adapter.PostgreSQLAdapter()
        dest_con.execute(sql_adapter.definition(dest_table))

    def fast_row_count(
        self, *, table_name: str, schema_name: typing.Optional[str] = None
    ) -> int:
        pass

    def inspect_table(self):
        pass

    def table_exists(
        self, *, table_name: str, schema_name: typing.Optional[str] = None
    ) -> bool:
        pass

    def upsert_table(
        self,
        *,
        src_repo: adapter.Repository,
        dest_sql_adapter: adapter.SqlAdapter,
        dest_con: adapter.DbConnection,
        dest_schema_name: str,
        dest_table_name: str,
        add: bool = True,
        update: bool = True,
        delete: bool = True,
    ) -> None:
        with dest_con:
            dest_table = src_repo.table.copy(
                new_schema_name=dest_schema_name,
                new_table_name=dest_table_name,
            )
            dest_table_exists = inspector.table_exists(
                con=dest_con,
                sql_adapter=dest_sql_adapter,
                schema_name=dest_schema_name,
                table_name=dest_table_name,
            )
            if not dest_table_exists:
                full_upload = True
                copy_table_structure(
                    src_table=src_repo.table,
                    dest_con=dest_con,
                    dest_schema_name=dest_schema_name,
                    dest_table_name=dest_table_name,
                )
            else:
                full_upload = False

            dest_repo = adapter.Repository(
                change_tracking_columns=src_repo.change_tracking_columns,
                connection=dest_con,
                sql_adapter=dest_sql_adapter,
                table=dest_table,
                read_only=False,
            )

            if full_upload:
                src_rows = src_repo.all()
                dest_repo.add(src_rows)
            else:
                changes = domain.compare_rows(
                    common_key_cols=src_repo.table.primary_key_column_names,
                    src_rows=src_repo.keys(True),
                    dest_rows=dest_repo.keys(True),
                )
                common_cols = src_repo.table.column_names.intersection(
                    dest_table.column_names
                )
                if changes["added"].row_count and add:
                    new_rows = src_repo.fetch_rows_by_primary_key_values(
                        rows=changes["added"], columns=common_cols
                    )
                    dest_repo.add(new_rows)
                if changes["deleted"].row_count and delete:
                    dest_repo.delete(changes["deleted"])
                if changes["updated"].row_count and update:
                    updated_rows = src_repo.fetch_rows_by_primary_key_values(
                        rows=changes["updated"], columns=common_cols
                    )
                    dest_repo.update(updated_rows)


