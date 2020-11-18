import abc

__all__ = ("Db",)

from py_db_adapter import domain


class Db(abc.ABC):
    def create_table(self, /, table: domain.Table) -> None:
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

    def table_exists(self, table_name: str, schema_name: typing.Optional[str] = None) -> bool:
        raise NotImplementedError



def fast_row_count(
        self, schema_name: typing.Optional[str], table_name: str
) -> typing.Optional[int]:
    table_name = self.full_table_name(schema_name=schema_name, table_name=table_name)
    return f"""
        SELECT
            (reltuples / relpages) *
            (
                pg_relation_size('{self.full_table_name}') / 
                current_setting('block_size')::INTEGER
            ) AS rows
        FROM pg_class
        WHERE
            relname = '{table_name}'
    """


def fast_row_count(
        self, schema_name: typing.Optional[str], table_name: str
) -> typing.Optional[int]:
    """A faster row-count method than .row_count(), but is only an estimate"""
    sql = f"DESCRIBE EXTENDED {self.full_table_name(schema_name=schema_name, table_name=table_name)}"
    result = self._con.execute(sql)
    for row in result.as_tuples():
        if row[0] == "Detailed Table Information":
            num_rows_match = re.search(".*, numRows=(\d+), .*", row[1])
            if num_rows_match:
                return int(num_rows_match.group(1))
    return None


def inspect_table(
        *,
        con: pyodbc.Connection,
        schema_name: str,
        table_name: str,
        custom_pk_cols: typing.List[str],
        cache_dir: typing.Optional[pathlib.Path] = None,
) -> domain.Table:
    if cache_dir:
        return adapter.pyodbc_inspect_table_and_cache(
            con=con,
            table_name=table_name,
            schema_name=schema_name,
            custom_pk_cols=custom_pk_cols,
            cache_dir=cache_dir,
        )
    else:
        return adapter.pyodbc_inspect_table(
            con=con,
            table_name=table_name,
            schema_name=schema_name,
            custom_pk_cols=custom_pk_cols,
        )


def table_exists(
        *,
        con: adapter.DbConnection,
        sql_adapter: adapter.SqlAdapter,
        schema_name: typing.Optional[str],
        table_name: str,
) -> bool:
    result = con.execute(
        sql_adapter.table_exists(schema_name=schema_name, table_name=table_name)
    )
    return bool(not result.is_empty and result.first_value())

