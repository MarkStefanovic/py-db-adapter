import typing

from py_db_adapter.service import db_service, DbService

__all__ = ("PostgresPyodbcDbService",)


class PostgresPyodbcDbService(db_service.DbService):
    def copy_table(
        self,
        *,
        src_db: DbService,
        src_schema_name: typing.Optional[str],
        src_table_name: str,
        dest_schema_name: typing.Optional[str],
        dest_table_name: str,
        columns: typing.Optional[typing.Set[str]] = None
    ) -> None:
        pass

    def fast_row_count(
        self, *, table_name: str, schema_name: typing.Optional[str]
    ) -> int:
        pass

    def inspect_table(self):
        pass

    def table_exists(
        self, *, table_name: str, schema_name: typing.Optional[str]
    ) -> bool:
        pass

    def upsert_table(
        self,
        *,
        src_db: DbService,
        src_schema_name: typing.Optional[str],
        src_table_name: str,
        dest_schema_name: typing.Optional[str],
        dest_table_name: str,
        compare_cols: typing.Optional[typing.Set[str]] = None,
        add: bool = True,
        update: bool = True,
        delete: bool = True
    ) -> None:
        pass
