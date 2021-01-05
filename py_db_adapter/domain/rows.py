from __future__ import annotations

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

    def as_tuples(self) -> typing.List[Row]:
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
    ) -> row_diff.RowDiff:
        return row_diff.RowDiff(
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
