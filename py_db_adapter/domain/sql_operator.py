import enum

__all__ = ("SqlOperator",)


class SqlOperator(str, enum.Enum):
    EQUALS = "="
    GREATER_THAN = ">"
    GREATER_THAN_OR_EQUAL_TO = ">="
    LESS_THAN = "<"
    LESS_THAN_OR_EQUAL_TO = "<="
    LIKE = "LIKE"

    def __str__(self) -> str:
        return str.__str__(self)
