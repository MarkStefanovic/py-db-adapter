import dataclasses


__all__ = ("ChangeTrackingResult",)

import datetime

import typing


@dataclasses.dataclass(frozen=True, eq=True)
class ChangeTrackingResult:
    new_rows: int
    soft_deletes: int
    new_row_versions: int
    batch_utc_millis_since_epoch: int
    hist_table_created: bool
    pk_cols_inferred: bool
    include_cols_inferred: bool
    error_message: typing.Optional[str]
    traceback: typing.Optional[str]

    @property
    def batch_ts(self) -> datetime.datetime:
        millis = self.batch_utc_millis_since_epoch / 1000.0
        return datetime.datetime.fromtimestamp(millis)

    @property
    def is_error(self) -> bool:
        return self.error_message is not None

    @property
    def is_success(self) -> bool:
        return self.error_message is None

    def __str__(self) -> str:
        return f"""{self.__class__.__name__}: Success
        batch_ts: {self.batch_ts.strftime("%Y-%m-%d %H:%M:%S.%f")}
        new_rows: {self.new_rows}
        soft_deleted: {self.soft_deletes}
        new_row_versions: {self.new_row_versions}
        hist_table_created: {self.hist_table_created}
        pk_cols_inferred: {self.pk_cols_inferred}
        include_cols_inferred: {self.include_cols_inferred}
        error_message: {self.error_message}
        traceback: {self.traceback}
        """
