import dataclasses

from py_db_adapter.domain.rows import Rows


__all__ = ("RowDiff",)


@dataclasses.dataclass(frozen=True, eq=True)
class RowDiff:
    rows_added: Rows
    rows_deleted: Rows
    rows_updated: Rows
