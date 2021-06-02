import typing
import warnings

from py_db_adapter.domain.row_diff import RowDiff
from py_db_adapter.domain.rows import Rows, rows_from_lookup_table, rows_to_lookup_table

__all__ = ("compare_rows",)


def compare_rows(
    *,
    key_cols: typing.Set[str],
    src_rows: Rows,
    dest_rows: Rows,
    compare_cols: typing.Optional[typing.Set[str]] = None,
) -> RowDiff:
    src_cols = set(src_rows.column_names)
    dest_cols = set(dest_rows.column_names)
    if compare_cols is None:
        common_cols = src_cols & dest_cols
    else:
        common_cols = key_cols | compare_cols
    common_compare_cols = common_cols - key_cols
    common_key_cols = key_cols & common_cols

    src_lkp_tbl = rows_to_lookup_table(
        rs=src_rows,
        key_columns=common_key_cols,
        value_columns=common_compare_cols,
    )
    dest_lkp_tbl = rows_to_lookup_table(
        rs=dest_rows,
        key_columns=common_key_cols,
        value_columns=common_compare_cols,
    )
    src_key_set = set(src_lkp_tbl.keys())
    dest_key_set = set(dest_lkp_tbl.keys())
    added = {k: src_lkp_tbl[k] for k in (src_key_set - dest_key_set)}
    deleted = {k: src_lkp_tbl.get(k, tuple()) for k in (dest_key_set - src_key_set)}
    if common_compare_cols:
        updates = {
            k: src_lkp_tbl.get(k, tuple())
            for k in src_key_set
            if k not in added
            and k not in deleted
            and src_lkp_tbl.get(k, tuple()) != dest_lkp_tbl.get(k, tuple())
        }
    else:
        warnings.warn(
            "There were no common comparison columns, so no updates can be calculated."
        )
        updates = {}
    return RowDiff(
        rows_added=rows_from_lookup_table(
            lookup_table=added,
            key_columns=common_key_cols,
            value_columns=common_compare_cols,
        ),
        rows_deleted=rows_from_lookup_table(
            lookup_table=deleted,
            key_columns=common_key_cols,
            value_columns=common_compare_cols,
        ),
        rows_updated=rows_from_lookup_table(
            lookup_table=updates,
            key_columns=common_key_cols,
            value_columns=common_compare_cols,
        ),
    )
