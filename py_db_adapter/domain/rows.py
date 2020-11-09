from __future__ import annotations

import typing

__all__ = (
    "Row",
    "Rows",
)


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

    @property
    def column_names(self) -> typing.List[str]:
        return self._column_names

    @classmethod
    def from_dicts(
        cls, /, rows: typing.List[typing.Dict[str, typing.Hashable]]
    ) -> Rows:
        column_names = sorted(rows[0].keys())
        rows = (
            tuple(typing.cast(typing.Hashable, v) for _, v in sorted(row.items()))
            for row in rows
        )
        return Rows(column_names=column_names, rows=rows)

    def as_dicts(self) -> typing.List[typing.Dict[str, typing.Hashable]]:
        return [dict(sorted(zip(self._column_names, row))) for row in self._rows]

    def as_lookup_table(
        self,
        *,
        key_columns: typing.Collection[str],
        value_columns: typing.Optional[typing.Collection[str]] = None,
    ) -> typing.Dict[typing.Tuple[typing.Hashable, ...], Row]:
        pk_cols = set(key_columns)
        if value_columns:
            value_cols = set(value_columns)
        else:
            value_cols = {col for col in self._column_names if col not in pk_cols}
        lkp_tbl = {}
        for row in self._rows:
            kv_pairs = list(zip(self._column_names, row))
            k = tuple(v for k, v in kv_pairs if k in pk_cols)
            if value_columns:
                v = tuple(v for k, v in kv_pairs if k in value_cols)
            else:
                v = tuple(v for k, v in kv_pairs if k not in pk_cols)
            lkp_tbl[k] = v
        return lkp_tbl

    def as_tuples(self) -> typing.List[Row]:
        return self._rows

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
