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

# SEE: SELECT * FROM pg_get_keywords ORDER BY 1;
POSTGRES_RESERVED_KEYWORDS = {
    "abort",
    "absolute",
    "access",
    "action",
    "add",
    "admin",
    "after",
    "aggregate",
    "all",
    "also",
    "alter",
    "always",
    "analyse",
    "analyze",
    "and",
    "any",
    "array",
    "as",
    "asc",
    "assertion",
    "assignment",
    "asymmetric",
    "at",
    "attach",
    "attribute",
    "authorization",
    "backward",
    "before",
    "begin",
    "between",
    "bigint",
    "binary",
    "bit",
    "boolean",
    "both",
    "by",
    "cache",
    "call",
    "called",
    "cascade",
    "cascaded",
    "case",
    "cast",
    "catalog",
    "chain",
    "char",
    "character",
    "characteristics",
    "check",
    "checkpoint",
    "class",
    "close",
    "cluster",
    "coalesce",
    "collate",
    "collation",
    "column",
    "columns",
    "comment",
    "comments",
    "commit",
    "committed",
    "concurrently",
    "configuration",
    "conflict",
    "connection",
    "constraint",
    "constraints",
    "content",
    "continue",
    "conversion",
    "copy",
    "cost",
    "create",
    "cross",
    "csv",
    "cube",
    "current",
    "current_catalog",
    "current_date",
    "current_role",
    "current_schema",
    "current_time",
    "current_timestamp",
    "current_user",
    "cursor",
    "cycle",
    "data",
    "database",
    "day",
    "deallocate",
    "dec",
    "decimal",
    "declare",
    "default",
    "defaults",
    "deferrable",
    "deferred",
    "definer",
    "delete",
    "delimiter",
    "delimiters",
    "depends",
    "desc",
    "detach",
    "dictionary",
    "disable",
    "discard",
    "distinct",
    "do",
    "document",
    "domain",
    "double",
    "drop",
    "each",
    "else",
    "enable",
    "encoding",
    "encrypted",
    "end",
    "enum",
    "escape",
    "event",
    "except",
    "exclude",
    "excluding",
    "exclusive",
    "execute",
    "exists",
    "explain",
    "extension",
    "external",
    "extract",
    "false",
    "family",
    "fetch",
    "filter",
    "first",
    "float",
    "following",
    "for",
    "force",
    "foreign",
    "forward",
    "freeze",
    "from",
    "full",
    "function",
    "functions",
    "generated",
    "global",
    "grant",
    "granted",
    "greatest",
    "group",
    "grouping",
    "groups",
    "handler",
    "having",
    "header",
    "hold",
    "hour",
    "identity",
    "if",
    "ilike",
    "immediate",
    "immutable",
    "implicit",
    "import",
    "in",
    "include",
    "including",
    "increment",
    "index",
    "indexes",
    "inherit",
    "inherits",
    "initially",
    "inline",
    "inner",
    "inout",
    "input",
    "insensitive",
    "insert",
    "instead",
    "int",
    "integer",
    "intersect",
    "interval",
    "into",
    "invoker",
    "is",
    "isnull",
    "isolation",
    "join",
    "key",
    "label",
    "language",
    "large",
    "last",
    "lateral",
    "leading",
    "leakproof",
    "least",
    "left",
    "level",
    "like",
    "limit",
    "listen",
    "load",
    "local",
    "localtime",
    "localtimestamp",
    "location",
    "lock",
    "locked",
    "logged",
    "mapping",
    "match",
    "materialized",
    "maxvalue",
    "method",
    "minute",
    "minvalue",
    "mode",
    "month",
    "move",
    "name",
    "names",
    "national",
    "natural",
    "nchar",
    "new",
    "next",
    "no",
    "none",
    "not",
    "nothing",
    "notify",
    "notnull",
    "nowait",
    "null",
    "nullif",
    "nulls",
    "numeric",
    "object",
    "of",
    "off",
    "offset",
    "oids",
    "old",
    "on",
    "only",
    "operator",
    "option",
    "options",
    "or",
    "order",
    "ordinality",
    "others",
    "out",
    "outer",
    "over",
    "overlaps",
    "overlay",
    "overriding",
    "owned",
    "owner",
    "parallel",
    "parser",
    "partial",
    "partition",
    "passing",
    "password",
    "placing",
    "plans",
    "policy",
    "position",
    "preceding",
    "precision",
    "prepare",
    "prepared",
    "preserve",
    "primary",
    "prior",
    "privileges",
    "procedural",
    "procedure",
    "procedures",
    "program",
    "publication",
    "quote",
    "range",
    "read",
    "real",
    "reassign",
    "recheck",
    "recursive",
    "ref",
    "references",
    "referencing",
    "refresh",
    "reindex",
    "relative",
    "release",
    "rename",
    "repeatable",
    "replace",
    "replica",
    "reset",
    "restart",
    "restrict",
    "returning",
    "returns",
    "revoke",
    "right",
    "role",
    "rollback",
    "rollup",
    "routine",
    "routines",
    "row",
    "rows",
    "rule",
    "savepoint",
    "schema",
    "schemas",
    "scroll",
    "search",
    "second",
    "security",
    "select",
    "sequence",
    "sequences",
    "serializable",
    "server",
    "session",
    "session_user",
    "set",
    "setof",
    "sets",
    "share",
    "show",
    "similar",
    "simple",
    "skip",
    "smallint",
    "snapshot",
    "some",
    "sql",
    "stable",
    "standalone",
    "start",
    "statement",
    "statistics",
    "stdin",
    "stdout",
    "storage",
    "stored",
    "strict",
    "strip",
    "subscription",
    "substring",
    "support",
    "symmetric",
    "sysid",
    "system",
    "table",
    "tables",
    "tablesample",
    "tablespace",
    "temp",
    "template",
    "temporary",
    "text",
    "then",
    "ties",
    "time",
    "timestamp",
    "to",
    "trailing",
    "transaction",
    "transform",
    "treat",
    "trigger",
    "trim",
    "true",
    "truncate",
    "trusted",
    "type",
    "types",
    "unbounded",
    "uncommitted",
    "unencrypted",
    "union",
    "unique",
    "unknown",
    "unlisten",
    "unlogged",
    "until",
    "update",
    "user",
    "using",
    "vacuum",
    "valid",
    "validate",
    "validator",
    "value",
    "values",
    "varchar",
    "variadic",
    "varying",
    "verbose",
    "version",
    "view",
    "views",
    "volatile",
    "when",
    "where",
    "whitespace",
    "window",
    "with",
    "within",
    "without",
    "work",
    "wrapper",
    "write",
    "xml",
    "xmlattributes",
    "xmlconcat",
    "xmlelement",
    "xmlexists",
    "xmlforest",
    "xmlnamespaces",
    "xmlparse",
    "xmlpi",
    "xmlroot",
    "xmlserialize",
    "xmltable",
    "year",
    "yes",
    "zone",
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
                self.column_sql_adapters, key=lambda c: c.column_metadata.column_name
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
