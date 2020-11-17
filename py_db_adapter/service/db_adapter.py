import abc


class DbAdapterService(abc.ABC):
    def fast_row_count(self) -> int:
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
        table_name: str
) -> bool:
    result = con.execute(sql_adapter.table_exists(schema_name=schema_name, table_name=table_name))
    return bool(not result.is_empty and result.first_value())



def upsert_table(
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
                key_cols=src_repo.table.primary_key_column_names,
                src_rows=src_repo.keys(True),
                dest_rows=dest_repo.keys(True),
            )
            common_cols = src_repo.table.column_names.intersection(dest_table.column_names)
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


def copy_table_structure(
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
