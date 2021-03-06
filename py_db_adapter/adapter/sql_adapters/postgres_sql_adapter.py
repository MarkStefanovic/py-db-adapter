from __future__ import annotations

import typing

from py_db_adapter.domain import (
    column as col,
    column_adapters,
    sql_adapter,
    std_column_adapters,
)

__all__ = (
    "PostgreSQLAdapter",
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
    ".cache",
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


class PostgreSQLAdapter(sql_adapter.SqlAdapter):
    def __init__(self) -> None:
        super().__init__(max_float_literal_decimal_places=5)

    def create_boolean_column(
        self, /, column: col.Column
    ) -> PostgresBooleanColumnSqlAdapter:
        return PostgresBooleanColumnSqlAdapter(column=column, wrapper=self.wrap)

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
        return PostgresIntegerColumnSqlAdapter(column=column, wrapper=self.wrap)

    def create_text_column(
        self, /, column: col.Column
    ) -> column_adapters.TextColumnSqlAdapter:
        return std_column_adapters.StandardTextColumnSqlAdapter(
            col=column, wrapper=self.wrap
        )

    def fast_row_count(
        self, *, schema_name: typing.Optional[str], table_name: str
    ) -> str:
        if schema_name:
            return f"""
                SELECT t.n_live_tup
                FROM pg_stat_all_tables t
                WHERE  
                    t.schemaname = '{schema_name}'
                    AND t.relname = '{table_name}'
                    AND (
                        t.last_analyze IS NOT NULL 
                        OR t.last_autoanalyze IS NOT NULL
                    )
            """
        else:
            return f"""
                SELECT t.n_live_tup
                FROM pg_stat_all_tables t
                WHERE  
                    t.relname = '{table_name}'
                    AND (
                        t.last_analyze IS NOT NULL 
                        OR t.last_autoanalyze IS NOT NULL
                    )
            """

    def table_exists(self, schema_name: typing.Optional[str], table_name: str) -> str:
        return (
            f"SELECT CASE WHEN to_regclass('{self.full_table_name(schema_name=schema_name, table_name=table_name)}') "
            f"IS NULL THEN 0 ELSE 1 END;"
        )

    def truncate_table(
        self, *, schema_name: typing.Optional[str], table_name: str
    ) -> str:
        full_table_name = self.full_table_name(
            schema_name=schema_name, table_name=table_name
        )
        return f"TRUNCATE TABLE {full_table_name}"

    def wrap(self, obj_name: str) -> str:
        if " " in obj_name or obj_name.lower() in POSTGRES_RESERVED_KEYWORDS:
            return f'"{obj_name}"'
        else:
            return obj_name


class PostgresBooleanColumnSqlAdapter(column_adapters.BooleanColumnSqlAdapter):
    def __init__(
        self,
        *,
        column: col.Column,
        wrapper: typing.Callable[[str], str],
    ):
        super().__init__(col=column, wrapper=wrapper)

    @property
    def definition(self) -> str:
        nullable = "NULL" if self._column.nullable else "NOT NULL"
        return f"{self.wrapped_column_name} BOOL {nullable}"

    def literal(self, value: bool) -> str:
        return "TRUE" if value else "FALSE"


class PostgresIntegerColumnSqlAdapter(column_adapters.IntegerColumnSqlAdapter):
    def __init__(
        self,
        *,
        column: col.Column,
        wrapper: typing.Callable[[str], str],
    ):
        super().__init__(col=column, wrapper=wrapper)

    @property
    def definition(self) -> str:
        if self.column_metadata.autoincrement:
            return f"{self.wrapped_column_name} SERIAL"
        else:
            return f"{self.wrapped_column_name} BIGINT {self.nullable}"

    def literal(self, value: int) -> str:
        return str(value)
