import typing

from py_db_adapter import domain

__all__ = ("PostgresTableInspector",)


class PostgresTableInspector(domain.TableInspector):
    def __init__(self, /, db: domain.DbConnection):
        self._db = db

    def inspect_table(
        self,
        *,
        schema_name: typing.Optional[str],
        table_name: str,
        custom_pk_cols: typing.Set[str],
        include_cols: typing.Set[str]
    ) -> domain.Table:
        pass
