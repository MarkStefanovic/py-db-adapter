import dataclasses
import typing


@dataclasses.dataclass(frozen=True)
class UniqueContraint:
    columns: typing.Tuple[str, ...]

    def definition(self, *, wrapper: typing.Callable[[str], str]) -> str:
        return f"UNIQUE ({', '.join(wrapper(col) for col in self.columns)})"
