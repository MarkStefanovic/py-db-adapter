import dataclasses
import decimal
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
    if pk_cols is None:
        src_pks = adapter.get_primary_key_cols_for_table(
            cur=src_cur,
            table_name=src_table_name,
            schema_name=src_schema_name,
        )
        dest_pks = adapter.get_primary_key_cols_for_table(
            cur=dest_cur,
            table_name=dest_table_name,
            schema_name=dest_schema_name,
        )
        if src_pks and dest_pks:
            pk_cols = [pk for pk in src_pks if pk in dest_pks]
        elif src_pks:
            pk_cols = src_pks
        elif dest_pks:
            pk_cols = dest_pks
        else:
            raise domain.exceptions.MissingPrimaryKey(
                schema_name=src_schema_name, table_name=src_table_name
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

    diff = domain.compare_rows(
        src_rows=src_rows,
        dest_rows=dest_rows,
        key_cols=pks,
        compare_cols=compare_cols,
        ignore_missing_key_cols=False,
        ignore_extra_key_cols=False,
    )

    if diff.rows_added:
        missing_row_examples = diff.rows_added.subset(column_names=pks).as_dicts()
        capped_missing_row_examples = {
            pk_col: pk_val
            for row in missing_row_examples
            for pk_col, pk_val in sorted(row.items())
        }
        missing_row_examples = f"({', '.join(sorted(pks))}): " + ", ".join(
            k for k, v in capped_missing_row_examples.items()[:max_examples]
        )
        if src_rows.row_count > 0:
            pct_missing = decimal.Decimal(
                format(diff.rows_added.row_count / src_rows.row_count, ".3f")
            )
        else:
            pct_missing = decimal.Decimal(0)
    else:
        missing_row_examples = ""
        pct_missing = decimal.Decimal(0)

    if diff.rows_deleted:
        extra_row_examples = diff.rows_deleted.subset(column_names=pks).as_dicts()
        capped_extra_row_examples = {
            pk_col: pk_val
            for row in extra_row_examples
            for pk_col, pk_val in sorted(row.items())
        }
        extra_row_examples = f"({', '.join(sorted(pks))}): " + ", ".join(
            k for k, v in capped_extra_row_examples.items()[:max_examples]
        )
        if src_rows.row_count > 0:
            pct_extra = decimal.Decimal(
                format(diff.rows_deleted.row_count / src_rows.row_count, ".3f")
            )
        else:
            pct_extra = decimal.Decimal(0)
    else:
        extra_row_examples = ""
        pct_extra = decimal.Decimal(0)

    if diff.rows_updated:
        stale_row_examples = diff.rows_updated.subset(column_names=pks).as_dicts()
        capped_stale_row_examples = {
            pk_col: pk_val
            for row in stale_row_examples
            for pk_col, pk_val in sorted(row.items())
        }
        stale_row_examples = f"({', '.join(sorted(pks))}): " + ", ".join(
            k for k, v in capped_stale_row_examples.items()[:max_examples]
        )
        if src_rows.row_count > 0:
            pct_stale = decimal.Decimal(
                format(diff.rows_updated.row_count / src_rows.row_count, ".3f")
            )
        else:
            pct_stale = decimal.Decimal(0)
    else:
        stale_row_examples = ""
        pct_stale = 0

    return domain.RowComparisonResult(
        src_schema=src_schema_name,
        src_table=src_table_name,
        dest_schema=dest_schema_name,
        dest_table=dest_table_name,
        missing_rows=diff.rows_added.row_count,
        missing_row_examples=missing_row_examples,
        pct_missing=pct_missing,
        extra_rows=diff.rows_deleted.row_count,
        extra_row_examples=extra_row_examples,
        pct_extra=pct_extra,
        stale_rows=diff.rows_updated.row_count,
        stale_row_examples=stale_row_examples,
        pct_stale=pct_stale,
    )
