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
