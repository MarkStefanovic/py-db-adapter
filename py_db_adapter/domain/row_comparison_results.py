import dataclasses
import datetime
import decimal

__all__ = ("RowComparisonResult",)

import typing


@dataclasses.dataclass(frozen=True, eq=True)
class RowComparisonResult:
    src_schema: str
    src_table: str
    dest_schema: str
    dest_table: str
    src_rows: int
    dest_rows: int
    missing_rows: int
    missing_row_examples: str
    pct_missing: decimal.Decimal
    extra_rows: int
    extra_row_examples: str
    pct_extra: decimal.Decimal
    stale_rows: int
    stale_row_examples: str
    pct_stale: decimal.Decimal
    ts: datetime.datetime
    error_message: typing.Optional[str]
    traceback: typing.Optional[typing.Tuple[str, ...]]

    @property
    def is_error(self) -> bool:
        return self.error_message is not None

    @property
    def is_success(self) -> bool:
        return self.error_message is None

    def __str__(self) -> str:
        return f"""{self.__class__.__name__}
        ts: {self.ts.strftime("%Y-%m-%d %H:%M:%S.%f")}
        src_schema: {self.src_schema}
        src_table: {self.src_table}
        src_rows: {self.src_rows}
        dest_rows: {self.dest_rows}
        dest_schema: {self.dest_schema}
        dest_table: {self.dest_table}
        missing_rows: {self.missing_rows}
        missing_row_examples: {self.missing_row_examples}
        pct_missing: {self.pct_missing * 100:.0f}%
        extra_rows: {self.extra_rows}
        extra_row_examples: {self.extra_row_examples}
        pct_extra: {self.pct_extra * 100:.0f}%
        stale_rows: {self.stale_rows}
        stale_row_examples: {self.stale_row_examples}
        pct_stale: {self.pct_stale * 100:.0f}%
        error_message: {self.error_message}
        traceback: {self.traceback}
        """
