"""
DB-API v2: https://www.python.org/dev/peps/pep-0249/

ref: see columns section of https://code.google.com/archive/p/pyodbc/wikis/Cursor.wiki

"""
from __future__ import annotations

import dataclasses
import pprint
import typing

import pydantic
import pyodbc

from py_db_adapter import domain
from py_db_adapter.domain import exceptions

__all__ = ("inspect_pyodbc_table", "table_exists",)


def inspect_pyodbc_table(
    con: pyodbc.Connection,
    table_name: str,
    schema_name: typing.Optional[str] = None,
) -> domain.Table:
    if not table_exists(con=con, table_name=table_name, schema_name=schema_name):
        raise exceptions.TableDoesNotExist(table_name=table_name, schema_name=schema_name)

    domain_cols = []
    pyodbc_cols = _inspect_cols(con=con, table_name=table_name, schema_name=schema_name)
    pk_cols = _inspect_pks(con=con, table_name=table_name, schema_name=schema_name)
    pk_col_names = {col.column_name for col in pk_cols}
    for col in pyodbc_cols:
        pk_col = col.column_name in pk_col_names
        if col.domain_data_type == domain.DataType.Bool:
            domain_col = domain.BooleanColumn(
                schema_name=schema_name,
                table_name=table_name,
                column_name=col.column_name,
                nullable=col.nullable_flag,
                primary_key=pk_col,
            )
        elif col.domain_data_type == domain.DataType.Date:
            domain_col = domain.DateColumn(
                schema_name=schema_name,
                table_name=table_name,
                column_name=col.column_name,
                nullable=col.nullable_flag,
                primary_key=pk_col,
            )
        elif col.domain_data_type == domain.DataType.DateTime:
            domain_col = domain.DateTimeColumn(
                schema_name=schema_name,
                table_name=table_name,
                column_name=col.column_name,
                nullable=col.nullable_flag,
                primary_key=pk_col,
            )
        elif col.domain_data_type == domain.DataType.Decimal:
            domain_col = domain.DecimalColumn(
                schema_name=schema_name,
                table_name=table_name,
                column_name=col.column_name,
                nullable=col.nullable_flag,
                primary_key=pk_col,
                precision=col.precision,
                scale=col.scale,
            )
        elif col.domain_data_type == domain.DataType.Float:
            domain_col = domain.FloatColumn(
                schema_name=schema_name,
                table_name=table_name,
                column_name=col.column_name,
                nullable=col.nullable_flag,
                primary_key=pk_col,
            )
        elif col.domain_data_type == domain.DataType.Int:
            domain_col = domain.IntegerColumn(
                autoincrement=col.autoincrement_flag,
                schema_name=schema_name,
                table_name=table_name,
                column_name=col.column_name,
                nullable=col.nullable_flag,
                primary_key=pk_col,
            )
        elif col.domain_data_type == domain.DataType.Text:
            domain_col = domain.TextColumn(
                schema_name=schema_name,
                table_name=table_name,
                column_name=col.column_name,
                nullable=col.nullable_flag,
                primary_key=pk_col,
                max_length=col.length,
            )
        else:
            raise ValueError(
                f"Unrecognized domain_data_type: {col.domain_data_type!r}"
            )

        domain_cols.append(domain_col)

    return domain.Table(
        schema_name=schema_name,
        table_name=table_name,
        columns=domain_cols,
    )


@pydantic.dataclasses.dataclass(frozen=True)
class PyodbcColumn:
    auto_increment: int
    base_typeid: int
    char_octet_length: int
    column_def: typing.Optional[str]
    column_name: str
    data_type: int
    display_size: int
    field_type: int
    is_nullable: typing.Optional[int]
    length: int
    nullable: int
    ordinal_position: int
    physical_number: int
    precision: int
    radix: typing.Optional[int]
    remarks: str
    scale: typing.Optional[int]
    sql_data_type: int
    sql_datetime_sub: typing.Optional[int]
    table_info: int
    table_oid: int
    table_name: str
    table_owner: str
    table_qualifier: str
    type_name: str
    typmod: int

    @property
    def domain_data_type(self) -> domain.DataType:
        if self.type_name == "bool":
            return domain.DataType.Bool
        else:
            return {
                -10: lambda: domain.DataType.Text,  # 'text'
                -11: lambda: domain.DataType.Text,  # 'uuid'
                -1: lambda: domain.DataType.Text,  # 'text'
                -3: lambda: domain.DataType.Text,  # 'bytea'
                -4: lambda: domain.DataType.Text,  # 'bytea'
                -5: lambda: domain.DataType.Int,  # 'int8'
                -6: lambda: domain.DataType.Int,  # 'int2'
                -7: lambda: domain.DataType.Bool,  # 'bool'
                -8: lambda: domain.DataType.Text,  # 'char'
                -9: lambda: domain.DataType.Text,  # 'varchar'
                10: lambda: domain.DataType.DateTime,  # 'time'
                11: lambda: domain.DataType.DateTime,  # 'timestamptz'
                12: lambda: domain.DataType.Text,  # 'varchar'
                1: lambda: domain.DataType.Text,  # 'char'
                2: lambda: domain.DataType.Decimal,  # 'numeric'
                3: lambda: domain.DataType.Decimal,  # 'numeric'
                4: lambda: domain.DataType.Int,  # 'int4'
                5: lambda: domain.DataType.Int,  # 'int2'
                6: lambda: domain.DataType.Float,  # 'float8'
                7: lambda: domain.DataType.Float,  # 'float4'
                8: lambda: domain.DataType.Float,  # 'float8'
                91: lambda: domain.DataType.Date,  # 'date'
                92: lambda: domain.DataType.DateTime,  # 'time'
                93: lambda: domain.DataType.DateTime,  # 'timestamptz'
                9: lambda: domain.DataType.Date,  # 'date'
            }[self.data_type]()

    @property
    def autoincrement_flag(self) -> bool:
        if self.auto_increment == 0:
            return False
        elif self.auto_increment == 1:
            return True
        else:
            raise ValueError(
                f"col.nullable should have been 0 or 1, but got {self.nullable}."
            )

    @property
    def nullable_flag(self) -> bool:
        if self.nullable == 0:
            return False
        elif self.nullable == 1:
            return True
        else:
            raise ValueError(
                f"col.nullable should have been 0 or 1, but got {self.nullable}."
            )

    @property
    def db_name(self) -> str:
        return self.table_qualifier

    @property
    def schema_name(self) -> str:
        return self.table_owner

    def __repr__(self):
        return repr(dataclasses.asdict(self))


@pydantic.dataclasses.dataclass(frozen=True)
class PyodbcDataInfo:
    type_name: str
    data_type: int
    precision: str
    literal_prefix: typing.Optional[str]
    literal_suffix: typing.Optional[str]
    create_params: typing.Optional[str]  # eg 'precision, scale' for the numeric type
    nullable: int
    case_sensitive: int
    searchable: int
    unsigned_attribute: typing.Optional[int]
    money: int
    auto_increment: typing.Optional[int]  # aka, auto_unique_value
    local_type_name: typing.Optional[str]
    minimum_scale: typing.Optional[int]
    maximum_scale: typing.Optional[int]
    sql_data_type: int
    sql_datetime_sub: typing.Optional[str]
    num_prec_radix: typing.Optional[int]
    interval_precision: int

    def __repr__(self):
        return repr(dataclasses.asdict(self))


@pydantic.dataclasses.dataclass(frozen=True)
class PyodbcPrimaryKeys:
    column_name: str
    key_seq: int
    pk_name: str
    table_name: str
    table_owner: str
    table_qualifier: str

    @property
    def db_name(self) -> str:
        return self.table_qualifier

    @property
    def schema_name(self) -> str:
        return self.table_owner

    def __repr__(self):
        return repr(dataclasses.asdict(self))


def _inspect_cols(
    con: pyodbc.Connection, table_name: str, schema_name: typing.Optional[str]
) -> typing.List[PyodbcColumn]:
    with con.cursor() as cur:
        return [
            PyodbcColumn(
                auto_increment=col.auto_increment,
                base_typeid=getattr(col, "base typeid"),
                char_octet_length=col.char_octet_length,
                column_def=col.column_def,
                column_name=col.column_name,
                data_type=col.data_type,
                display_size=col.display_size,
                field_type=col.field_type,
                is_nullable=col.is_nullable,
                length=col.length,
                nullable=col.nullable,
                ordinal_position=col.ordinal_position,
                physical_number=getattr(col, "physical number"),
                precision=col.precision,
                radix=col.radix,
                remarks=col.remarks,
                scale=col.scale,
                sql_data_type=col.sql_data_type,
                sql_datetime_sub=col.sql_datetime_sub,
                table_info=getattr(col, "table info"),
                table_oid=getattr(col, "table oid"),
                table_name=col.table_name,
                table_owner=col.table_owner,
                table_qualifier=col.table_qualifier,
                type_name=col.type_name,
                typmod=col.typmod,
            )
            for col in cur.columns(table_name, schema=schema_name)
        ]


def _inspect_pks(
    con: pyodbc.Connection, table_name: str, schema_name: typing.Optional[str]
) -> typing.List[PyodbcPrimaryKeys]:
    with con.cursor() as cur:
        return [
            PyodbcPrimaryKeys(
                column_name=col.column_name,
                key_seq=col.key_seq,
                pk_name=col.pk_name,
                table_name=col.table_name,
                table_owner=col.table_owner,
                table_qualifier=col.table_qualifier,
            )
            for col in cur.primaryKeys(table_name, schema=schema_name)
        ]


def table_exists(con: pyodbc.Connection, table_name: str, schema_name: str) -> bool:
    with con.cursor() as cur:
        return bool(
            cur.tables(
                table=table_name, schema=schema_name, tableType="TABLE"
            ).fetchone()
        )


if __name__ == "__main__":
    con_str = conn_str = (
        "DRIVER={PostgreSQL Unicode(x64)};"
        "DATABASE=dummy;"
        "UID=marks;"
        "PWD=bumblebee;"
        "SERVER=localhost;"
        "PORT=5432;"
    )
    schema = "hr"
    table = "employee"
    with pyodbc.connect(con_str) as con:
        tbl = inspect_pyodbc_table(con=con, table_name=table, schema_name=schema)
        pprint.pprint(tbl)
