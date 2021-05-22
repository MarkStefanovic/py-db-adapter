from __future__ import annotations

import datetime
import typing

from py_db_adapter.domain import (
    column as col,
    column_adapters,
    sql_adapter,
    std_column_adapters,
)

__all__ = ("SqlServerSQLAdapter",)

RESERVED_KEYWORDS = {
    "procedure",
    "all",
    "fetch",
    "public",
    "alter",
    "file",
    "raiserror",
    "and",
    "fillfactor",
    "read",
    "any",
    "for",
    "readtext",
    "as",
    "foreign",
    "reconfigure",
    "asc",
    "freetext",
    "references",
    "authorization",
    "freetexttable",
    "replication",
    "backup",
    "from",
    "restore",
    "begin",
    "full",
    "restrict",
    "between",
    "function",
    "return",
    "break",
    "goto",
    "revert",
    "browse",
    "grant",
    "revoke",
    "bulk",
    "group",
    "right",
    "by",
    "having",
    "rollback",
    "cascade",
    "holdlock",
    "rowcount",
    "case",
    "identity",
    "rowguidcol",
    "check",
    "identity_insert",
    "rule",
    "checkpoint",
    "identitycol",
    "save",
    "close",
    "if",
    "schema",
    "clustered",
    "in",
    "securityaudit",
    "coalesce",
    "index",
    "select",
    "collate",
    "inner",
    "semantickeyphrasetable",
    "column",
    "insert",
    "semanticsimilaritydetailstable",
    "commit",
    "intersect",
    "semanticsimilaritytable",
    "compute",
    "into",
    "session_user",
    "constraint",
    "is",
    "set",
    "contains",
    "join",
    "setuser",
    "containstable",
    "key",
    "shutdown",
    "continue",
    "kill",
    "some",
    "convert",
    "left",
    "statistics",
    "create",
    "like",
    "system_user",
    "cross",
    "lineno",
    "table",
    "current",
    "load",
    "tablesample",
    "current_date",
    "merge",
    "textsize",
    "current_time",
    "national",
    "then",
    "current_timestamp",
    "nocheck",
    "to",
    "current_user",
    "nonclustered",
    "top",
    "cursor",
    "not",
    "tran",
    "database",
    "null",
    "transaction",
    "dbcc",
    "nullif",
    "trigger",
    "deallocate",
    "of",
    "truncate",
    "declare",
    "off",
    "try_convert",
    "default",
    "offsets",
    "tsequal",
    "delete",
    "on",
    "union",
    "deny",
    "open",
    "unique",
    "desc",
    "opendatasource",
    "unpivot",
    "disk",
    "openquery",
    "update",
    "distinct",
    "openrowset",
    "updatetext",
    "distributed",
    "openxml",
    "use",
    "double",
    "option",
    "user",
    "drop",
    "or",
    "values",
    "dump",
    "order",
    "varying",
    "else",
    "outer",
    "view",
    "end",
    "over",
    "waitfor",
    "errlvl",
    "percent",
    "when",
    "escape",
    "pivot",
    "where",
    "except",
    "plan",
    "while",
    "exec",
    "precision",
    "with",
    "execute",
    "primary",
    "within",
    "group",
    "exists",
    "print",
    "writetext",
    "exit",
    "proc",
}


class SqlServerSQLAdapter(sql_adapter.SqlAdapter):
    def __init__(self) -> None:
        super().__init__(max_float_literal_decimal_places=5)

    def create_boolean_column(
        self, /, column: col.Column
    ) -> std_column_adapters.StandardBooleanColumnSqlAdapter:
        return std_column_adapters.StandardBooleanColumnSqlAdapter(
            col=column, wrapper=self.wrap
        )

    def create_date_column(
        self, /, column: col.Column
    ) -> column_adapters.DateColumnSqlAdapter:
        return std_column_adapters.StandardDateColumnSqlAdapter(
            col=column, wrapper=self.wrap
        )

    def create_datetime_column(
        self, /, column: col.Column
    ) -> column_adapters.DateTimeColumnSqlAdapter:
        return std_column_adapters.StandardDateTimeColumnSqlAdapter(
            col=column, wrapper=self.wrap
        )

    def create_decimal_column(
        self, /, column: col.Column
    ) -> column_adapters.DecimalColumnSqlAdapter:
        return std_column_adapters.StandardDecimalColumnSqlAdapter(
            col=column, wrapper=self.wrap
        )

    def create_float_column(
        self, /, column: col.Column
    ) -> column_adapters.FloatColumnSqlAdapter:
        return std_column_adapters.StandardFloatColumnSqlAdapter(
            col=column,
            wrapper=self.wrap,
            max_decimal_places=self.max_float_literal_decimal_places,
        )

    def create_integer_column(
        self, /, column: col.Column
    ) -> column_adapters.IntegerColumnSqlAdapter:
        return std_column_adapters.StandardIntegerColumnSqlAdapter(
            col=column, wrapper=self.wrap
        )

    def create_text_column(
        self, /, column: col.Column
    ) -> column_adapters.TextColumnSqlAdapter:
        return std_column_adapters.StandardTextColumnSqlAdapter(
            col=column, wrapper=self.wrap
        )

    def fast_row_count(
        self, *, schema_name: typing.Optional[str], table_name: str
    ) -> str:
        full_table_name = self.full_table_name(
            schema_name=schema_name, table_name=table_name
        )
        return f"""
            SELECT SUM (row_count)
            FROM sys.dm_db_partition_stats
            WHERE
                object_id = OBJECT_ID('{full_table_name}')
                AND (index_id=0 or index_id=1)
        """

    def table_exists(self, schema_name: typing.Optional[str], table_name: str) -> str:
        full_table_name = self.full_table_name(
            schema_name=schema_name, table_name=table_name
        )
        return (
            f"SELECT CASE WHEN OBJECT_ID('{full_table_name}') IS NULL THEN 0 ELSE 1 END"
        )

    def truncate(self, *, schema_name: typing.Optional[str], table_name: str) -> str:
        full_table_name = self.full_table_name(
            schema_name=schema_name, table_name=table_name
        )
        return f"TRUNCATE TABLE {full_table_name}"

    def wrap(self, obj_name: str) -> str:
        if " " in obj_name or obj_name.lower() in RESERVED_KEYWORDS:
            return f"[{obj_name}]"
        else:
            return obj_name


class SqlServerDateTimeColumnSqlAdapter(column_adapters.DateTimeColumnSqlAdapter):
    def __init__(self, *, column: col.Column, wrapper: typing.Callable[[str], str]):
        super().__init__(col=column, wrapper=wrapper)

    @property
    def definition(self) -> str:
        return f"{self.wrapped_column_name} DATETIME {self.nullable}"

    def literal(self, value: datetime.datetime) -> str:
        date_str = value.strftime("%Y-%m-%d %H:%M:%S")
        return f"CAST({date_str!r} AS DATETIME)"
