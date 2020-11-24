from __future__ import annotations

import itertools
import typing

__all__ = (
    "Row",
    "Rows",
)

from py_db_adapter.domain import row_diff

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

    def as_dicts(self) -> typing.List[typing.Dict[str, typing.Hashable]]:
        return [dict(sorted(zip(self._column_names, row))) for row in self._rows]

    def as_lookup_table(
        self,
        *,
        key_columns: typing.Set[str],
        value_columns: typing.Optional[typing.Set[str]] = None,
    ) -> typing.Dict[Row, Row]:
        pk_cols = sorted(set(key_columns))
        if value_columns:
            value_cols = sorted(set(value_columns))
        else:
            value_cols = sorted(
                {col for col in self._column_names if col not in pk_cols}
            )
        return {
            tuple(row[self.column_indices[col_name]] for col_name in pk_cols): tuple(
                row[self.column_indices[col_name]] for col_name in value_cols
            )
            for row in self._rows
        }

    def as_tuples(self) -> typing.List[Row]:
        return self._rows

    def batches(self, /, size: int) -> typing.Generator[Rows, typing.Any, None]:
        chunks = (self._rows[i : i + size] for i in range(0, len(self._rows), size))
        for chunk in chunks:
            yield Rows(column_names=self._column_names, rows=chunk)

    def column(self, /, column_name: str) -> typing.List[typing.Hashable]:
        col_index = self.column_indices[column_name]
        return [row[col_index] for row in self._rows]

    @property
    def column_names(self) -> typing.List[str]:
        return self._column_names

    @property
    def column_indices(self) -> typing.Dict[str, int]:
        return {col_name: i for i, col_name in enumerate(self._column_names)}

    def compare(
        self,
        *,
        rows: Rows,
        key_cols: typing.Set[str],
        compare_cols: typing.Set[str],
        ignore_missing_key_cols: bool,
        ignore_extra_key_cols: bool,
    ) -> row_diff.RowDiff:
        return row_diff.RowDiff(
            key_cols=key_cols,
            compare_cols=compare_cols,
            src_rows=self,
            dest_rows=rows,
            ignore_missing_key_cols=ignore_missing_key_cols,
            ignore_extra_key_cols=ignore_extra_key_cols,
        )

    @staticmethod
    def concat(rows: typing.List[Rows]) -> Rows:
        column_names = rows[0].column_names
        all_rows = [row for batch in rows for row in batch.as_tuples()]
        return Rows(
            column_names=column_names,
            rows=all_rows,
        )

    @classmethod
    def from_dicts(
        cls, /, rows: typing.List[typing.Dict[str, typing.Hashable]]
    ) -> Rows:
        column_names = sorted(rows[0].keys())
        new_rows = [tuple(v for _, v in sorted(row.items())) for row in rows]
        return Rows(column_names=column_names, rows=new_rows)

    def first_value(self) -> typing.Any:
        return self._rows[0][0]

    @classmethod
    def from_lookup_table(
        cls,
        *,
        lookup_table: typing.Dict[Row, Row],
        key_columns: typing.Set[str],
        value_columns: typing.Set[str],
    ) -> Rows:
        ordered_key_col_names = sorted(key_columns)
        ordered_value_col_names = sorted(value_columns)
        column_names = ordered_key_col_names + ordered_value_col_names
        rows = [
            tuple(itertools.chain(keys, values))
            for keys, values in lookup_table.items()
        ]
        return Rows(column_names=column_names, rows=rows)

    @property
    def is_empty(self) -> bool:
        return not self._rows

    @property
    def row_count(self) -> int:
        return len(self._rows)

    def subset(self, column_names: typing.Set[str]) -> Rows:
        cols = sorted(column_names)
        rows = [
            tuple(row[self.column_indices[col_name]] for col_name in cols)
            for row in self._rows
        ]
        return Rows(
            column_names=cols,
            rows=rows,
        )

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
