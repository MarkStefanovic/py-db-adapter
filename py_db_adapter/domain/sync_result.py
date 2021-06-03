import dataclasses
import typing

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
    skipped_reason: typing.Optional[str]
    error_message: typing.Optional[str]
    traceback: typing.Optional[typing.Tuple[str]]

    @property
    def is_error(self) -> bool:
        return self.error_message is not None

    @property
    def is_success(self) -> bool:
        return self.error_message is None

    def __str__(self) -> str:
        return f"""{self.__class__.__name__}:
        src_schema_name: {self.src_schema_name}
        src_table_name: {self.src_table_name}
        dest_schema_name: {self.dest_schema_name}
        dest_table_name: {self.dest_table_name}
        added: {self.added}
        deleted: {self.deleted}
        updated: {self.updated}
        skipped: {self.skipped}
        error_message: {self.error_message}
        traceback: {self.traceback}
        """
