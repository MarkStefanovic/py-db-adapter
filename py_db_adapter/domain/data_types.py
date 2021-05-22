import datetime
import decimal
import enum

__all__ = ("DataType",)


class DataType(enum.Enum):
    Bool = "bool"
    Date = "date"
    DateTime = "datetime"
    Decimal = "decimal"
    Float = "float"
    Int = "int"
    Text = "str"

    @property
    def py_type(self) -> type:
        return {
            DataType.Bool: bool,
            DataType.Date: datetime.date,
            DataType.DateTime: datetime.datetime,
            DataType.Decimal: decimal.Decimal,
            DataType.Float: float,
            DataType.Int: int,
            DataType.Text: str,
        }[self.value]
