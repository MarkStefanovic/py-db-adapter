import dataclasses
import typing


@dataclasses.dataclass(frozen=True)
class Index:
    schema_name: typing.Optional[str]
    table_name: str
    columns: typing.List[str]
    unique: bool

    def definition(self, *, wrapper: typing.Callable[[str], str]) -> str:
        col_csv = ", ".join(wrapper(col) for col in self.columns)
        if self.schema_name:
            full_table_name = f"{wrapper(self.schema_name)}.{wrapper(self.table_name)}"
        else:
            full_table_name = wrapper(self.table_name)
        chars_left_for_name = 63 - len(f"XX_{self.table_name}")
        snake_cols = "_".join(self.columns)[:chars_left_for_name]
        if self.unique:
            ix_prefix = "UNIQUE"
            ix_name_prefix = f"uq"
        else:
            ix_prefix = ""
            ix_name_prefix = "ix"
        return (
            f"CREATE {ix_prefix} INDEX IF NOT EXISTS {ix_name_prefix}_{self.table_name}_{snake_cols} ON "
            f"{full_table_name} ({col_csv})"
        )
