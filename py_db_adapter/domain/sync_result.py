import dataclasses
import typing

from py_db_adapter.domain.rows import Rows

__all__ = ("SyncResult",)


@dataclasses.dataclass(frozen=True, eq=True)
class SyncResult:
    src_schema_name: typing.Optional[str]
    src_table_name: str
    dest_schema_name: typing.Optional[str]
    dest_table_name: str
    added: int
    deleted: int
    updated: int
    skipped: bool
