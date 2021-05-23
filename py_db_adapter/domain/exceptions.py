import pathlib
import typing

__all__ = (
    "PyDbAdapterException",
    "DatabaseIsReadOnly",
    "ExtraKeyColumns",
    "FastRowCountFailed",
    "MissingPrimaryKey",
    "MissingKeyColumns",
    "NoCommonKeyColumns",
    "InvalidCustomPrimaryKey",
    "TableDoesNotExist",
    "TableMissingPrimaryKey",
    "TableHasNoColumns",
)


class PyDbAdapterException(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)


class ColumnNameNotFound(PyDbAdapterException):
    def __init__(
        self, column_name: str, table_name: str, available_cols: typing.Set[str]
    ):
        self.column_name = column_name
        self.table_name = table_name
        self.available_cols = available_cols
        msg = (
            f"Could not find a column named {column_name} for table {table_name}.  Available columns include the "
            f"following: {', '.join(available_cols)}."
        )
        super().__init__(msg)


class DatabaseIsReadOnly(PyDbAdapterException):
    def __init__(self) -> None:
        super().__init__("The database is read only.")


class DirectoryDoesNotExit(PyDbAdapterException):
    def __init__(self, *, folder: pathlib.Path):
        self.folder = folder
        super().__init__(f"The folder, {folder!s}, does not exist.")


class ExtraKeyColumns(PyDbAdapterException):
    def __init__(
        self, *, actual_key_cols: typing.Set[str], expected_key_cols: typing.Set[str]
    ):
        self.actual_key_cols = actual_key_cols
        self.expected_key_cols = expected_key_cols
        msg = (
            f"Extra key columns found.  The following key columns were expected, {', '.join(sorted(actual_key_cols))}, "
            f"but got {', '.join(sorted(expected_key_cols))}."
        )
        super().__init__(msg)


class FastRowCountFailed(PyDbAdapterException):
    def __init__(self, table_name: str, error_message: str):
        msg = f"fast_row_count() failed for the {table_name} table with the following error message:\n{error_message}"
        super().__init__(msg)

        self.table_name = table_name


class InvalidCustomPrimaryKey(PyDbAdapterException):
    def __init__(self, invalid_column_names: typing.Iterable[str]) -> None:
        msg = (
            f"The following custom primary key columns do not match columns on the table: "
            f"{', '.join(invalid_column_names)}."
        )
        super().__init__(msg)
        self.invalid_column_names = invalid_column_names


class InvalidSqlGenerated(PyDbAdapterException):
    def __init__(self, sql: str, message: str):
        self.sql = sql
        super().__init__(message)


class MissingColumns(PyDbAdapterException):
    def __init__(self, /, missing_cols: typing.Set[str]):
        self.missing_cols = missing_cols
        msg = f"The following required columns are missing: {', '.join(sorted(missing_cols))}"
        super().__init__(msg)


class MissingPrimaryKey(PyDbAdapterException):
    def __init__(self, schema_name: typing.Optional[str], table_name: str) -> None:
        full_table_name = f"{schema_name}.{table_name}" if schema_name else table_name
        msg = f"{full_table_name} is missing a primary key."
        super().__init__(msg)

        self.schema_name = schema_name
        self.table_name = table_name


class NoCommonKeyColumns(PyDbAdapterException):
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


class MissingKeyColumns(PyDbAdapterException):
    def __init__(
        self, *, actual_key_cols: typing.Set[str], expected_key_cols: typing.Set[str]
    ):
        self.actual_key_cols = actual_key_cols
        self.expected_key_cols = expected_key_cols
        msg = (
            f"Key columns missing.  The following key columns were expected, {', '.join(sorted(actual_key_cols))}, "
            f"but got {', '.join(sorted(expected_key_cols))}."
        )
        super().__init__(msg)


class SchemaIsRequired(PyDbAdapterException):
    def __init__(self, message: str) -> None:
        super().__init__(message)


class TableDoesNotExist(PyDbAdapterException):
    def __init__(self, table_name: str, schema_name: typing.Optional[str]) -> None:
        full_table_name = f"{schema_name}.{table_name}" if schema_name else table_name
        super().__init__(f"The table, {full_table_name}, does not exist.")
        self.schema_name = schema_name
        self.table_name = table_name


class TableMissingPrimaryKey(PyDbAdapterException):
    def __init__(self, table_name: str, schema_name: typing.Optional[str]):
        full_table_name = f"{schema_name}.{table_name}" if schema_name else table_name
        super().__init__(f"The table, {full_table_name}, is missing a primary key.")
        self.schema_name = schema_name
        self.table_name = table_name


class TableHasNoColumns(PyDbAdapterException):
    def __init__(self, table_name: str, schema_name: typing.Optional[str]):
        full_table_name = f"{schema_name}.{table_name}" if schema_name else table_name
        super().__init__(
            f"The table, {full_table_name}, has no columns associated with it."
        )
        self.schema_name = schema_name
        self.table_name = table_name
