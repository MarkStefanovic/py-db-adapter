import abc
import dataclasses
import typing


from py_db_adapter.domain import exceptions

__all__ = ("PrimaryKey",)


@dataclasses.dataclass(frozen=True, eq=True)
class PrimaryKey(abc.ABC):
    schema_name: typing.Optional[str]
    table_name: str
    columns: typing.Tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.columns:
            raise exceptions.TableMissingPrimaryKey(
                schema_name=self.schema_name, table_name=self.table_name
            )

    def definition(self, *, wrapper: typing.Callable[[str], str]) -> str:
        return f"PRIMARY KEY ({', '.join(wrapper(col) for col in self.columns)})"
