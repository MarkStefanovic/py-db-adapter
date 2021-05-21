from __future__ import annotations

import itertools
import typing
import warnings

import pyodbc

from py_db_adapter.domain import exceptions

__all__ = ("Row", "Rows", "RowDiff")

Row = typing.Tuple[typing.Any, ...]


class Rows:
    def __init__(
        self,
        *,
        column_names: typing.Iterable[str],
        rows: typing.Iterable[Row],
    ):
        self._column_names = list(column_names)
        self._rows = list(rows)

        self._column_indices = {
            col_name: i for i, col_name in enumerate(self._column_names)
        }

    def add_column_from_callable(
        self,
        *,
        column_name: str,
        fn: typing.Callable[[typing.Dict[str, typing.Any]], typing.Any],
    ) -> Rows:
        return Rows(
            column_names=self._column_names + [column_name],
            rows=[
                tuple(row_dict.values()) + fn(row_dict) for row_dict in self.as_dicts()
            ],
        )

    def add_static_column(
        self,
        *,
        column_name: str,
        value: typing.Any,
    ) -> Rows:
        return Rows(
            column_names=self._column_names + [column_name],
            rows=[row + (value,) for row in self._rows],
        )

    def as_dicts(self) -> typing.List[typing.Dict[str, typing.Hashable]]:
        return [dict(sorted(zip(self._column_names, row))) for row in self._rows]

    def as_tuples(self, *, sort_columns: bool = True) -> typing.List[Row]:
        if sort_columns:
            ordered_col_indices = [
                self._column_indices[col_name]
                for col_name in sorted(self._column_names)
            ]
            return [tuple(row[i] for i in ordered_col_indices) for row in self._rows]
        return self._rows

    def batches(self, /, size: int) -> typing.Generator[Rows, typing.Any, None]:
        chunks = (self._rows[i : i + size] for i in range(0, len(self._rows), size))
        for chunk in chunks:
            yield Rows(column_names=self._column_names, rows=chunk)

    def column(self, /, column_name: str) -> typing.List[typing.Hashable]:
        col_index = self._column_indices[column_name]
        return [row[col_index] for row in self._rows]

    @property
    def column_names(self) -> typing.List[str]:
        return self._column_names

    def compare(
        self,
        *,
        rows: Rows,
        key_cols: typing.Set[str],
        compare_cols: typing.Set[str],
        ignore_missing_key_cols: bool,
        ignore_extra_key_cols: bool,
    ) -> RowDiff:
        return RowDiff(
            key_cols=key_cols,
            compare_cols=compare_cols,
            src_rows=rows,
            dest_rows=self,
            ignore_missing_key_cols=ignore_missing_key_cols,
            ignore_extra_key_cols=ignore_extra_key_cols,
        )

    @staticmethod
    def concat(rows: typing.List[Rows]) -> Rows:
        if rows:
            column_names = rows[0].column_names
            all_rows = [row for batch in rows for row in batch.as_tuples()]
            return Rows(
                column_names=column_names,
                rows=all_rows,
            )
        else:
            return Rows(column_names=[], rows=[])

    @classmethod
    def from_dicts(
        cls, /, rows: typing.List[typing.Dict[str, typing.Hashable]]
    ) -> Rows:
        if rows:
            column_names = sorted(rows[0].keys())
            new_rows = [tuple(v for _, v in sorted(row.items())) for row in rows]
            return Rows(column_names=column_names, rows=new_rows)
        else:
            return Rows(column_names=[], rows=[])

    def first_value(self) -> typing.Optional[typing.Any]:
        if self.is_empty:
            return None
        else:
            return self._rows[0][0]

    @property
    def is_empty(self) -> bool:
        return not self._rows

    @property
    def row_count(self) -> int:
        return len(self._rows)

    def subset(self, column_names: typing.Set[str]) -> Rows:
        cols = sorted(column_names)
        rows = [
            tuple(row[self._column_indices[col_name]] for col_name in cols)
            for row in self._rows
        ]
        return Rows(column_names=cols, rows=rows)

    def update_column_values(
        self,
        column_name: str,
        transform: typing.Callable[[typing.Dict[str, typing.Any]], typing.Any] = None,
        static_value: typing.Any = None,
    ) -> Rows:
        if transform is None:
            rows = [
                {
                    key: static_value if column_name == key else val
                    for ix, (key, val) in enumerate(row_dict.items())
                }
                for row_dict in self.as_dicts()
            ]
        else:
            rows = [
                {
                    key: transform(row_dict) if column_name == key else val
                    for ix, (key, val) in enumerate(row_dict.items())
                }
                for row_dict in self.as_dicts()
            ]
        return Rows.from_dicts(rows)

    def __eq__(self, other: typing.Any) -> bool:
        if other.__class__ is self.__class__:
            other = typing.cast(Rows, other)
            return self._rows == other._rows
        else:
            return NotImplemented

    def __hash__(self) -> int:
        return hash(tuple(row) for row in self._rows)

    def __repr__(self) -> str:
        return f"<Rows: {len(self._rows)} items>"

    def __str__(self) -> str:
        return str(self._rows)


class RowDiff:
    def __init__(
        self,
        *,
        key_cols: typing.Set[str],
        compare_cols: typing.Set[str],
        src_rows: Rows,
        dest_rows: Rows,
        ignore_missing_key_cols: bool = True,
        ignore_extra_key_cols: bool = True,
    ):
        self._key_cols = key_cols
        self._compare_cols = compare_cols
        self._src_rows = src_rows
        self._dest_rows = dest_rows
        self._ignore_missing_key_cols = ignore_missing_key_cols
        self._ignore_extra_key_cols = ignore_extra_key_cols

        self._rows_added: typing.Optional[Rows] = None
        self._rows_deleted: typing.Optional[Rows] = None
        self._rows_updated: typing.Optional[Rows] = None

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
    def rows_added(self) -> Rows:
        if self._rows_added is None:
            self._compare()
        assert self._rows_added is not None
        return self._rows_added

    @property
    def rows_deleted(self) -> Rows:
        if self._rows_deleted is None:
            self._compare()
        assert self._rows_deleted is not None
        return self._rows_deleted

    @property
    def rows_updated(self) -> Rows:
        if self._rows_updated is None:
            self._compare()
        assert self._rows_updated is not None
        return self._rows_updated


def compare_rows(
    *,
    key_cols: typing.Set[str],
    src_rows: Rows,
    dest_rows: Rows,
    ignore_missing_key_cols: bool = True,
    ignore_extra_key_cols: bool = True,
) -> typing.Dict[str, Rows]:
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
    lookup_table: typing.Dict[Row, Row],
    key_columns: typing.Set[str],
    value_columns: typing.Set[str],
) -> Rows:
    ordered_key_col_names = sorted(key_columns)
    ordered_value_col_names = sorted(value_columns)
    column_names = ordered_key_col_names + ordered_value_col_names
    new_rows = [
        tuple(itertools.chain(keys, values)) for keys, values in lookup_table.items()
    ]
    return Rows(column_names=column_names, rows=new_rows)


def rows_to_lookup_table(
    rs: Rows,
    key_columns: typing.Set[str],
    value_columns: typing.Optional[typing.Set[str]] = None,
) -> typing.Dict[Row, Row]:
    pk_cols = sorted(set(key_columns))
    if value_columns:
        value_cols = sorted(set(value_columns))
    else:
        value_cols = sorted({col for col in rs.column_names if col not in pk_cols})
    return {
        tuple(row[col] for col in pk_cols): tuple(row[col] for col in value_cols)
        for row in rs.as_dicts()
    }


def fetch_rows(con: pyodbc.Connection, sql: str, params: typing.Tuple[typing.Any]):
    @staticmethod
    def from_result_cursor(cursor: pyodbc.Cursor):
        column_names = [description[0] for description in cursor.description]
        if rows := result.fetchall():
            return domain.Rows(
                column_names=column_names, rows=[tuple(row) for row in rows]
            )
        else:
            return domain.Rows(column_names=column_names, rows=[])


if __name__ == "__main__":
    r = Rows(column_names=["test", "this"], rows=[("abc", "def")])
    print(f"{r=}")
