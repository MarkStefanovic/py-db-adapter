from __future__ import annotations

import itertools
import typing
import warnings

from py_db_adapter.domain import exceptions, rows

__all__ = ("RowDiff",)


class RowDiff:
    def __init__(
        self,
        *,
        key_cols: typing.Set[str],
        compare_cols: typing.Set[str],
        src_rows: rows.Rows,
        dest_rows: rows.Rows,
        ignore_missing_key_cols: bool = True,
        ignore_extra_key_cols: bool = True,
    ):
        self._key_cols = key_cols
        self._compare_cols = compare_cols
        self._src_rows = src_rows
        self._dest_rows = dest_rows
        self._ignore_missing_key_cols = ignore_missing_key_cols
        self._ignore_extra_key_cols = ignore_extra_key_cols

        self._rows_added: typing.Optional[rows.Rows] = None
        self._rows_deleted: typing.Optional[rows.Rows] = None
        self._rows_updated: typing.Optional[rows.Rows] = None

    def _compare(self) -> None:
        results = compare_rows(
            key_cols=self._key_cols,
            src_rows=self._src_rows,
            dest_rows=self._dest_rows,
            ignore_missing_key_cols=self._ignore_missing_key_cols,
            ignore_extra_key_cols=self._ignore_extra_key_cols,
        )
        self._rows_added = results["added"]
        self._rows_deleted = results["deleted"]
        self._rows_updated = results["updated"]

    @property
    def rows_added(self) -> rows.Rows:
        if self._rows_added is None:
            self._compare()
        assert self._rows_added is not None
        return self._rows_added

    @property
    def rows_deleted(self) -> rows.Rows:
        if self._rows_deleted is None:
            self._compare()
        assert self._rows_deleted is not None
        return self._rows_deleted

    @property
    def rows_updated(self) -> rows.Rows:
        if self._rows_updated is None:
            self._compare()
        assert self._rows_updated is not None
        return self._rows_updated


def compare_rows(
    *,
    key_cols: typing.Set[str],
    src_rows: rows.Rows,
    dest_rows: rows.Rows,
    ignore_missing_key_cols: bool = True,
    ignore_extra_key_cols: bool = True,
) -> typing.Dict[str, rows.Rows]:
    src_cols = set(src_rows.column_names)
    dest_cols = set(dest_rows.column_names)
    common_cols = src_cols & dest_cols
    common_compare_cols = common_cols - key_cols
    common_key_cols = key_cols & common_cols
    src_key_cols = key_cols & src_cols
    dest_key_cols = dest_cols & dest_cols
    if not common_key_cols:
        raise exceptions.NoCommonKeyColumns(
            src_key_cols=src_key_cols, dest_key_cols=dest_key_cols
        )

    if not ignore_missing_key_cols:
        if key_cols - src_key_cols:
            raise exceptions.MissingKeyColumns(
                actual_key_cols=src_key_cols, expected_key_cols=key_cols
            )
        if key_cols - dest_key_cols:
            raise exceptions.MissingKeyColumns(
                actual_key_cols=dest_key_cols, expected_key_cols=key_cols
            )

    if not ignore_extra_key_cols:
        if src_key_cols - key_cols:
            raise exceptions.ExtraKeyColumns(
                actual_key_cols=src_key_cols, expected_key_cols=key_cols
            )
        if dest_key_cols - key_cols:
            raise exceptions.ExtraKeyColumns(
                actual_key_cols=dest_key_cols, expected_key_cols=key_cols
            )

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
    return {
        "added": rows_from_lookup_table(
            lookup_table=added,
            key_columns=common_key_cols,
            value_columns=common_compare_cols,
        ),
        "deleted": rows_from_lookup_table(
            lookup_table=deleted,
            key_columns=common_key_cols,
            value_columns=common_compare_cols,
        ),
        "updated": rows_from_lookup_table(
            lookup_table=updates,
            key_columns=common_key_cols,
            value_columns=common_compare_cols,
        ),
    }


def rows_from_lookup_table(
    *,
    lookup_table: typing.Dict[rows.Row, rows.Row],
    key_columns: typing.Set[str],
    value_columns: typing.Set[str],
) -> rows.Rows:
    ordered_key_col_names = sorted(key_columns)
    ordered_value_col_names = sorted(value_columns)
    column_names = ordered_key_col_names + ordered_value_col_names
    new_rows = [
        tuple(itertools.chain(keys, values)) for keys, values in lookup_table.items()
    ]
    return rows.Rows(column_names=column_names, rows=new_rows)


def rows_to_lookup_table(
    rs: rows.Rows,
    key_columns: typing.Set[str],
    value_columns: typing.Optional[typing.Set[str]] = None,
) -> typing.Dict[rows.Row, rows.Row]:
    pk_cols = sorted(set(key_columns))
    if value_columns:
        value_cols = sorted(set(value_columns))
    else:
        value_cols = sorted({col for col in rs.column_names if col not in pk_cols})
    return {
        tuple(row[col] for col in pk_cols): tuple(row[col] for col in value_cols)
        for row in rs.as_dicts()
    }
