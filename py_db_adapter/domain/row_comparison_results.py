import dataclasses
import decimal

__all__ = ("RowComparisonResult",)


@dataclasses.dataclass(frozen=True, eq=True)
class RowComparisonResult:
    src_schema: str
    src_table: str
    dest_schema: str
    dest_table: str
    missing_rows: int
    missing_row_examples: str
    pct_missing: decimal.Decimal
    extra_rows: int
    extra_row_examples: str
    pct_extra: decimal.Decimal
    stale_rows: int
    stale_row_examples: str
    pct_stale: decimal.Decimal
