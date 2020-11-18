import logging
import typing

from py_db_adapter import domain
from py_db_adapter.domain import rows

__all__ = ("RowDiff",)

logger = logging.getLogger(__name__)


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

    def _compare(self):
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

    def rows_added(self) -> rows.Rows:
        if self._rows_added is None:
            self._compare()
        assert self._rows_added is not None
        return self._rows_added

    def rows_deleted(self) -> rows.Rows:
        if self._rows_deleted is None:
            self._compare()
        assert self._rows_deleted is not None
        return self._rows_deleted

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
        raise domain.exceptions.NoCommonKeyColumns(
            src_key_cols=src_key_cols, dest_key_cols=dest_key_cols
        )

    if not ignore_missing_key_cols:
        if key_cols - src_key_cols:
            raise domain.exceptions.MissingKeyColumns(
                actual_key_cols=src_key_cols, expected_key_cols=key_cols
            )
        if key_cols - dest_key_cols:
            raise domain.exceptions.MissingKeyColumns(
                actual_key_cols=src_key_cols, expected_key_cols=key_cols
            )

    if not ignore_extra_key_cols:
        if src_key_cols - key_cols:
            raise domain.exceptions.ExtraKeyColumns(
                actual_key_cols=src_key_cols, expected_key_cols=key_cols
            )
        if dest_key_cols - key_cols:
            raise domain.exceptions.ExtraKeyColumns(
                actual_key_cols=src_key_cols, expected_key_cols=key_cols
            )

    src_lkp_tbl = src_rows.as_lookup_table(
        key_columns=common_key_cols, value_columns=common_compare_cols
    )
    dest_lkp_tbl = dest_rows.as_lookup_table(
        key_columns=common_key_cols, value_columns=common_compare_cols
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
        logger.debug(
            "There were no common comparison columns, so no updates can be calculated."
        )
        updates = {}
    return {
        "added": rows.Rows.from_lookup_table(
            lookup_table=added,
            key_columns=common_key_cols,
            value_columns=common_compare_cols,
        ),
        "deleted": rows.Rows.from_lookup_table(
            lookup_table=deleted,
            key_columns=common_key_cols,
            value_columns=common_compare_cols,
        ),
        "updated": rows.Rows.from_lookup_table(
            lookup_table=updates,
            key_columns=common_key_cols,
            value_columns=common_compare_cols,
        ),
    }
