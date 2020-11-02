import typing


class PyDbAdapterException(Exception):
    def __init__(self, message: str):
        super().__init__(message)


class TableDoesNotExist(PyDbAdapterException):
    def __init__(self, table_name: str, schema_name: typing.Optional[str]):
        full_table_name = f"{schema_name}.{table_name}" if schema_name else table_name
        super().__init__(f"The table, {full_table_name}, does not exist.")
        self.schema_name = schema_name
        self.table_name = table_name
