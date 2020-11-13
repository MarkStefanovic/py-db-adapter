import typing


class PyDbAdapterException(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)


class MissingPrimaryKey(PyDbAdapterException):
    def __init__(self, schema_name: typing.Optional[str], table_name: str) -> None:
        full_table_name = f"{schema_name}.{table_name}" if schema_name else table_name
        msg = f"{full_table_name} is missing a primary key."
        super().__init__(msg)

        self.schema_name = schema_name
        self.table_name = table_name


class InvalidCustomPrimaryKey(PyDbAdapterException):
    def __init__(self, invalid_column_names: typing.Iterable[str]) -> None:
        msg = (
            f"The following custom primary key columns do not match columns on the table: "
            f"{', '.join(invalid_column_names)}."
        )
        super().__init__(msg)
        self.invalid_column_names = invalid_column_names


class DatabaseIsReadOnly(PyDbAdapterException):
    def __init__(self) -> None:
        super().__init__("The database is read only.")


class TableDoesNotExist(PyDbAdapterException):
    def __init__(self, table_name: str, schema_name: typing.Optional[str]) -> None:
        full_table_name = f"{schema_name}.{table_name}" if schema_name else table_name
        super().__init__(f"The table, {full_table_name}, does not exist.")
        self.schema_name = schema_name
        self.table_name = table_name
