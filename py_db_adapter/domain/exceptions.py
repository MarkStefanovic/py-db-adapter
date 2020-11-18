import typing


class PyDbAdapterException(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)


class DataError(PyDbAdapterException):
    def __init__(self, /, message: str):
        super().__init__(message)


class DeveloperError(PyDbAdapterException):
    def __init__(self, /, message: str):
        super().__init__(message)


class MissingPrimaryKey(DataError):
    def __init__(self, schema_name: typing.Optional[str], table_name: str) -> None:
        full_table_name = f"{schema_name}.{table_name}" if schema_name else table_name
        msg = f"{full_table_name} is missing a primary key."
        super().__init__(msg)

        self.schema_name = schema_name
        self.table_name = table_name


class MissingKeyColumns(DataError):
    def __init__(self, *, actual_key_cols: typing.Set[str], expected_key_cols: typing.Set[str]):
        self.actual_key_cols = actual_key_cols
        self.expected_key_cols = expected_key_cols
        msg = (
            f"Key columns missing.  The following key columns were expected, {', '.join(sorted(actual_key_cols))}, "
            f"but got {', '.join(sorted(expected_key_cols))}."
        )
        super().__init__(msg)


class ExtraKeyColumns(DataError):
    def __init__(self, *, actual_key_cols: typing.Set[str], expected_key_cols: typing.Set[str]):
        self.actual_key_cols = actual_key_cols
        self.expected_key_cols = expected_key_cols
        msg = (
            f"Extra key columns found.  The following key columns were expected, {', '.join(sorted(actual_key_cols))}, "
            f"but got {', '.join(sorted(expected_key_cols))}."
        )
        super().__init__(msg)


class NoCommonKeyColumns(DataError):
    def __init__(
        self, *, src_key_cols: typing.Set[str], dest_key_cols: typing.Set[str]
    ):
        self.src_key_cols = src_key_cols
        self.dest_key_cols = dest_key_cols
        msg = (
            f"There are no common key columns between source and destination:\n\t"
            f"source keys: {sorted(src_key_cols)}\n\tdest keys: {sorted(dest_key_cols)}"
        )
        super().__init__(msg)


class InvalidCustomPrimaryKey(DataError):
    def __init__(self, invalid_column_names: typing.Iterable[str]) -> None:
        msg = (
            f"The following custom primary key columns do not match columns on the table: "
            f"{', '.join(invalid_column_names)}."
        )
        super().__init__(msg)
        self.invalid_column_names = invalid_column_names


class DatabaseIsReadOnly(DeveloperError):
    def __init__(self) -> None:
        super().__init__("The database is read only.")


class TableDoesNotExist(DataError):
    def __init__(self, table_name: str, schema_name: typing.Optional[str]) -> None:
        full_table_name = f"{schema_name}.{table_name}" if schema_name else table_name
        super().__init__(f"The table, {full_table_name}, does not exist.")
        self.schema_name = schema_name
        self.table_name = table_name
