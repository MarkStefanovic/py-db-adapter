import dataclasses
import decimal
import itertools
import pathlib
import typing

import pyodbc

from py_db_adapter import adapter, domain


__all__ = ("compare_rows",)


def compare_rows(
    # fmt: off
    src_cur: pyodbc.Cursor,
    dest_cur: pyodbc.Cursor,
    src_db_adapter: domain.DbAdapter,
    dest_db_adapter: domain.DbAdapter,
    src_schema_name: typing.Optional[str],
    src_table_name: str,
    dest_schema_name: typing.Optional[str],
    dest_table_name: str,
    pk_cols: typing.Optional[typing.List[str]] = None,  # None = inspect to find out
    compare_cols: typing.Optional[typing.Set[str]] = None,  # None = compare on all common cols
    cache_dir: typing.Optional[pathlib.Path] = None,
    max_examples: int = 10,
    # fmt: on
) -> domain.RowComparisonResult:
    pk_cols = coalesce_pks(
        pk_cols=pk_cols,
        src_cur=src_cur,
        dest_cur=dest_cur,
        src_schema_name=src_schema_name,
        src_table_name=src_table_name,
        dest_schema_name=dest_schema_name,
        dest_table_name=dest_table_name,
    )

    src_table = adapter.inspect_table(
        cur=src_cur,
        table_name=src_table_name,
        schema_name=src_schema_name,
        pk_cols=pk_cols,
        include_cols=None,
        cache_dir=cache_dir,
    )

    dest_table = adapter.inspect_table(
        cur=dest_cur,
        table_name=dest_table_name,
        schema_name=dest_schema_name,
        pk_cols=pk_cols,
        include_cols=None,
        cache_dir=cache_dir,
    )

    pks = set(pk_cols)

    src_table = dataclasses.replace(
        src_table,
        primary_key=domain.PrimaryKey(
            schema_name=src_schema_name,
            table_name=src_table_name,
            columns=tuple(sorted(pks)),
        ),
    )

    dest_table = dataclasses.replace(
        dest_table,
        primary_key=domain.PrimaryKey(
            schema_name=dest_schema_name,
            table_name=dest_table_name,
            columns=tuple(sorted(pks)),
        ),
    )

    if compare_cols is None:
        src_cols = src_table.non_pk_column_names
        dest_cols = dest_table.non_pk_column_names
        compare_cols = src_cols & dest_cols

    src_rows = src_db_adapter.table_keys(
        cur=src_cur,
        table=src_table,
        additional_cols=compare_cols,
    )

    dest_rows = dest_db_adapter.table_keys(
        cur=dest_cur,
        table=dest_table,
        additional_cols=compare_cols,
    )

    diff: domain.RowDiff = domain.compare_rows(
        src_rows=src_rows,
        dest_rows=dest_rows,
        key_cols=pks,
        compare_cols=compare_cols,
    )

    extra_rows = diff.rows_deleted
    missing_rows = diff.rows_added
    stale_rows = diff.rows_updated

    extra_pct = decimal.Decimal(
        format(extra_rows.row_count / src_rows.row_count, ".2f")
    )
    missing_pct = decimal.Decimal(
        format(missing_rows.row_count / src_rows.row_count, ".2f")
    )
    stale_pct = decimal.Decimal(
        format(stale_rows.row_count / src_rows.row_count, ".2f")
    )

    extra_examples = rows_to_examples(
        rows=extra_rows, pk_cols=pks, max_examples=max_examples
    )
    missing_examples = rows_to_examples(
        rows=missing_rows, pk_cols=pks, max_examples=max_examples
    )
    stale_examples = rows_to_examples(
        rows=stale_rows, pk_cols=pks, max_examples=max_examples
    )

    return domain.RowComparisonResult(
        src_schema=src_schema_name,
        src_table=src_table_name,
        dest_schema=dest_schema_name,
        dest_table=dest_table_name,
        missing_rows=diff.rows_added.row_count,
        missing_row_examples=missing_examples,
        pct_missing=missing_pct,
        extra_rows=diff.rows_deleted.row_count,
        extra_row_examples=extra_examples,
        pct_extra=extra_pct,
        stale_rows=diff.rows_updated.row_count,
        stale_row_examples=stale_examples,
        pct_stale=stale_pct,
    )


def coalesce_pks(
    *,
    pk_cols: typing.Optional[typing.List[str]],
    src_cur: pyodbc.Cursor,
    dest_cur: pyodbc.Cursor,
    src_schema_name: str,
    src_table_name: str,
    dest_schema_name: str,
    dest_table_name: str,
) -> typing.List[str]:
    if pk_cols is None:
        src_pks = adapter.get_primary_key_cols_for_table(
            cur=src_cur,
            table_name=src_table_name,
            schema_name=src_schema_name,
        )
        if src_pks:
            return src_pks
        else:
            dest_pks = adapter.get_primary_key_cols_for_table(
                cur=dest_cur,
                table_name=dest_table_name,
                schema_name=dest_schema_name,
            )
            if dest_pks:
                return dest_pks
            else:
                raise domain.exceptions.MissingPrimaryKey(
                    schema_name=src_schema_name, table_name=src_table_name
                )
    else:
        return pk_cols


def rows_to_examples(
    rows: domain.Rows, pk_cols: typing.Set[str], max_examples: int
) -> str:
    pks: typing.List[typing.Dict[str, typing.Any]] = rows.subset(pk_cols).as_dicts()
    if pks:
        prefix = "(" + ", ".join(pks[0].keys()) + "): "
        examples = [
            "(" + ", ".join(str(c) for c in row.values()) + ")"
            for row in pks[:max_examples]
        ]
        return prefix + ", ".join(str(x) for x in examples)
    else:
        return ""
