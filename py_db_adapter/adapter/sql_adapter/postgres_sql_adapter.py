from __future__ import annotations

import typing

from py_db_adapter import domain
from py_db_adapter.adapter.sql_adapter import (
    sql_column_adapter,
    sql_table_adapter,
)
from py_db_adapter.adapter import sql_formatter

__all__ = (
    "PostgreSQLTableAdapter",
    "PostgresBooleanColumnSqlAdapter",
)

# SEE: SELECT * FROM pg_get_keywords WHERE catdesc = 'reserved' ORDER BY 1;
POSTGRES_RESERVED_KEYWORDS = {
    "all",
    "analyse",
    "analyze",
    "and",
    "any",
    "array",
    "as",
    "asc",
    "asymmetric",
    "both",
    "case",
    "cast",
    "check",
    "collate",
    "column",
    "constraint",
    "create",
    "current_catalog",
    "current_date",
    "current_role",
    "current_time",
    "current_timestamp",
    "current_user",
    "default",
    "deferrable",
    "desc",
    "distinct",
    "do",
    "else",
    "end",
    "except",
    "false",
    "fetch",
    "for",
    "foreign",
    "from",
    "grant",
    "group",
    "having",
    "in",
    "initially",
    "intersect",
    "into",
    "lateral",
    "leading",
    "limit",
    "localtime",
    "localtimestamp",
    "not",
    "null",
    "offset",
    "on",
    "only",
    "or",
    "order",
    "placing",
    "primary",
    "references",
    "returning",
    "select",
    "session_user",
    "some",
    "symmetric",
    "table",
    "then",
    "to",
    "trailing",
    "true",
    "union",
    "unique",
    "user",
    "using",
    "variadic",
    "when",
    "where",
    "window",
    "with",
}


class PostgreSQLTableAdapter(sql_table_adapter.SqlTableAdapter):
    def __init__(self, table: domain.Table):
        super().__init__(table=table, max_float_literal_decimal_places=5)

    def create_boolean_column_sql_adapter(
        self, /, column: domain.BooleanColumn
    ) -> PostgresBooleanColumnSqlAdapter:
        return PostgresBooleanColumnSqlAdapter(column=column, wrapper=self.wrap)

    def create_date_column_sql_adapter(
        self, /, column: domain.DateColumn
    ) -> sql_column_adapter.DateColumnSqlAdapter:
        return sql_column_adapter.StandardDateColumnSqlAdapter(
            column=column, wrapper=self.wrap
        )

    def create_datetime_column_sql_adapter(
        self, /, column: domain.DateTimeColumn
    ) -> sql_column_adapter.DateTimeColumnSqlAdapter:
        return sql_column_adapter.StandardDateTimeColumnSqlAdapter(
            column=column, wrapper=self.wrap
        )

    def create_decimal_column_sql_adapter(
        self, /, column: domain.DecimalColumn
    ) -> sql_column_adapter.DecimalColumnSqlAdapter:
        return sql_column_adapter.StandardDecimalColumnSqlAdapter(
            column=column, wrapper=self.wrap
        )

    def create_float_column_sql_adapter(
        self, /, column: domain.FloatColumn
    ) -> sql_column_adapter.FloatColumnSqlAdapter:
        return sql_column_adapter.StandardFloatColumnSqlAdapter(
            column=column,
            wrapper=self.wrap,
            max_decimal_places=self.max_float_literal_decimal_places,
        )

    def create_integer_column_sql_adapter(
        self, /, column: domain.IntegerColumn
    ) -> sql_column_adapter.IntegerColumnSqlAdapter:
        return sql_column_adapter.StandardIntegerColumnSqlAdapter(
            column=column, wrapper=self.wrap
        )

    def create_text_column_sql_adapter(
        self, /, column: domain.TextColumn
    ) -> sql_column_adapter.TextColumnSqlAdapter:
        return sql_column_adapter.StandardTextColumnSqlAdapter(
            column=column, wrapper=self.wrap
        )

    @property
    def create(self) -> str:
        pk_col_names = sorted(
            col.column_name for col in self.table_metadata.columns if col.primary_key
        )
        pk_col_csv = ", ".join(self.wrap(col) for col in pk_col_names)
        col_csv = ", ".join(
            col.definition
            for col in sorted(
                self.column_sql_adapters, key=lambda c: c.column_metadata.column_name  # type: ignore
            )
        )
        return sql_formatter.standardize_sql(
            f"""CREATE TABLE IF NOT EXISTS {self.full_table_name} (
                {col_csv}
            ,   PRIMARY KEY ({pk_col_csv})
            )"""
        )

    @property
    def drop(self) -> str:
        return f"DROP TABLE IF EXISTS {self.full_table_name}"

    @property
    def truncate(self) -> str:
        return f"TRUNCATE TABLE {self.full_table_name}"

    def wrap(self, obj_name: str) -> str:
        if " " in obj_name or obj_name.lower() in POSTGRES_RESERVED_KEYWORDS:
            return f'"{obj_name}"'
        else:
            return obj_name


class PostgresBooleanColumnSqlAdapter(sql_column_adapter.BooleanColumnSqlAdapter):
    def __init__(
        self,
        column: domain.BooleanColumn,
        wrapper: typing.Callable[[str], str],
    ):
        super().__init__(column=column, wrapper=wrapper)

    @property
    def definition(self) -> str:
        nullable = "NULL" if self._column.nullable else "NOT NULL"
        return f"{self.wrapped_column_name} BOOL {nullable}"

    def literal(self, value: bool) -> str:
        return "TRUE" if value else "FALSE"
