"""
DB-API v2: https://www.python.org/dev/peps/pep-0249/

ref: see columns section of https://code.google.com/archive/p/pyodbc/wikis/Cursor.wiki

"""
from __future__ import annotations

import pathlib
import pickle
import typing

import pydantic
import pyodbc

from py_db_adapter import domain
from py_db_adapter.domain import exceptions

__all__ = (
    "pyodbc_inspect_table",
    "pyodbc_inspect_table_and_cache",
    "pyodbc_table_exists",
)


def pyodbc_inspect_table_and_cache(
    cache_dir: pathlib.Path,
    con: pyodbc.Connection,
    table_name: str,
    schema_name: typing.Optional[str] = None,
    custom_pk_cols: typing.Optional[typing.Set[str]] = None,
) -> domain.Table:
    fp = cache_dir / f"{schema_name}.{table_name}.p"
    if fp.exists():
        table = pickle.load(open(file=fp, mode="rb"))
    else:
        table = pyodbc_inspect_table(
            con=con,
            table_name=table_name,
            schema_name=schema_name,
            custom_pk_cols=custom_pk_cols,
        )
        pickle.dump(table, open(fp, "wb"))
    return table


def pyodbc_inspect_table(
    con: pyodbc.Connection,
    table_name: str,
    schema_name: typing.Optional[str] = None,
    custom_pk_cols: typing.Optional[typing.Set[str]] = None,
    sync_cols: typing.Optional[typing.Set[str]] = None,
) -> domain.Table:
    if not pyodbc_table_exists(con=con, table_name=table_name, schema_name=schema_name):
        raise exceptions.TableDoesNotExist(
            table_name=table_name, schema_name=schema_name
        )

    domain_cols = []
    pyodbc_cols = _inspect_cols(con=con, table_name=table_name, schema_name=schema_name)
    pk_cols = _inspect_pks(con=con, table_name=table_name, schema_name=schema_name)

    if not pk_cols and not custom_pk_cols:
        raise exceptions.MissingPrimaryKey(
            schema_name=schema_name, table_name=table_name
        )

    pk_col_names = {col.column_name for col in pk_cols}

    include_all_cols = not sync_cols
    for col in pyodbc_cols:
        if include_all_cols or col.column_name in sync_cols:
            if col.domain_data_type == domain.DataType.Bool:
                domain_col: domain.Column = domain.BooleanColumn(
                    schema_name=schema_name,
                    table_name=table_name,
                    column_name=col.column_name,
                    nullable=col.nullable_flag,
                    autoincrement=False,
                )
            elif col.domain_data_type == domain.DataType.Date:
                domain_col = domain.DateColumn(
                    schema_name=schema_name,
                    table_name=table_name,
                    column_name=col.column_name,
                    nullable=col.nullable_flag,
                    autoincrement=False,
                )
            elif col.domain_data_type == domain.DataType.DateTime:
                domain_col = domain.DateTimeColumn(
                    schema_name=schema_name,
                    table_name=table_name,
                    column_name=col.column_name,
                    nullable=col.nullable_flag,
                    autoincrement=False,
                )
            elif col.domain_data_type == domain.DataType.Decimal:
                domain_col = domain.DecimalColumn(
                    schema_name=schema_name,
                    table_name=table_name,
                    column_name=col.column_name,
                    nullable=col.nullable_flag,
                    precision=col.precision or 18,
                    scale=col.scale or 2,
                    autoincrement=False,
                )
            elif col.domain_data_type == domain.DataType.Float:
                domain_col = domain.FloatColumn(
                    schema_name=schema_name,
                    table_name=table_name,
                    column_name=col.column_name,
                    nullable=col.nullable_flag,
                    autoincrement=False,
                )
            elif col.domain_data_type == domain.DataType.Int:
                domain_col = domain.IntegerColumn(
                    autoincrement=col.autoincrement_flag,
                    schema_name=schema_name,
                    table_name=table_name,
                    column_name=col.column_name,
                    nullable=col.nullable_flag,
                )
            elif col.domain_data_type == domain.DataType.Text:
                domain_col = domain.TextColumn(
                    schema_name=schema_name,
                    table_name=table_name,
                    column_name=col.column_name,
                    nullable=col.nullable_flag,
                    max_length=col.length,
                    autoincrement=False,
                )
            else:
                raise ValueError(f"Unrecognized domain_data_type: {col.domain_data_type!r}")

            domain_cols.append(domain_col)

    if custom_pk_cols:
        col_names = {col.column_name for col in domain_cols}
        missing_pk_col_names = {col for col in pk_col_names if col not in col_names}
        if missing_pk_col_names:
            raise exceptions.InvalidCustomPrimaryKey(sorted(missing_pk_col_names))
        pk_col_names = custom_pk_cols

    return domain.Table(
        schema_name=schema_name,
        table_name=table_name,
        columns=set(domain_cols),
        pk_cols=pk_col_names,
    )


class PyodbcColumn(pydantic.BaseModel):
    auto_increment: int
    base_typeid: typing.Optional[int]
    char_octet_length: typing.Optional[int]
    column_def: typing.Optional[str]
    column_name: str
    data_type: int
    display_size: int
    field_type: int
    is_nullable: typing.Optional[int]
    length: typing.Optional[int]
    nullable: int
    ordinal_position: int
    physical_number: typing.Optional[int]
    precision: typing.Optional[int]
    radix: typing.Optional[int]
    remarks: str
    scale: typing.Optional[int]
    sql_data_type: int
    sql_datetime_sub: typing.Optional[int]
    table_info: typing.Optional[int]
    table_oid: typing.Optional[int]
    table_name: str
    table_owner: str
    table_qualifier: str
    type_name: str
    typmod: typing.Optional[int]

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
                f"auto_increment should have been 0 or 1, but got {self.auto_increment!r}."
            )

    @property
    def nullable_flag(self) -> bool:
        if self.nullable == 0:
            return False
        elif self.nullable == 1:
            return True
        else:
            raise ValueError(
                f"nullable should have been 0 or 1, but got {self.nullable!r}."
            )

    @property
    def db_name(self) -> str:
        return self.table_qualifier

    @property
    def schema_name(self) -> str:
        return self.table_owner

    def __repr__(self) -> str:
        return repr(self.dict())


class PyodbcDataInfo(pydantic.BaseModel):
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

    def __repr__(self) -> str:
        return repr(self.dict())


class PyodbcPrimaryKeys(pydantic.BaseModel):
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

    def __repr__(self) -> str:
        return repr(self.dict())


def _inspect_cols(
    con: pyodbc.Connection, table_name: str, schema_name: typing.Optional[str]
) -> typing.List[PyodbcColumn]:
    def handle_is_nullable(is_nullable: typing.Union[int, str]) -> bool:
        if isinstance(is_nullable, str):
            if is_nullable == "YES":
                return True
            elif is_nullable == "NO":
                return False
            else:
                raise ValueError(
                    f"Expected is_nullable to be either 'YES' or 'NO', but got {is_nullable!r}."
                )
        else:
            if is_nullable == 1:
                return True
            else:
                return False

    with con.cursor() as cur:
        # Hortonworks Hive ODBC Driver:
        #    doesn't include the following attributes:
        #       auto_increment
        #       base_typeid
        #       length
        #       physical number
        #       precision
        #       scale
        #       table info
        #       table oid
        #       typmod
        #    uses the following attribute names:
        #       column_size instead of display_size
        #       user_data_type instead of field_type
        #       num_prec_radix instead of radix
        return [
            PyodbcColumn(
                auto_increment=(
                    col.auto_increment if hasattr(col, "auto_increment") else 0
                ),
                base_typeid=(
                    getattr(col, "base typeid") if hasattr(col, "base typeid") else None
                ),
                char_octet_length=col.char_octet_length,
                column_def=col.column_def,
                column_name=col.column_name,
                data_type=col.data_type,
                display_size=(
                    col.display_size
                    if hasattr(col, "display_size")
                    else col.column_size
                ),
                field_type=(
                    col.field_type if hasattr(col, "field_type") else col.user_data_type
                ),
                is_nullable=handle_is_nullable(col.is_nullable),
                length=(col.length if hasattr(col, "length") else None),
                nullable=col.nullable,
                ordinal_position=col.ordinal_position,
                physical_number=(
                    getattr(col, "physical number")
                    if hasattr(col, "physical number")
                    else None
                ),
                precision=(col.precision if hasattr(col, "precision") else None),
                radix=(col.radix if hasattr(col, "radix") else col.num_prec_radix),
                remarks=col.remarks,
                scale=(col.scale if hasattr(col, "scale") else None),
                sql_data_type=col.sql_data_type,
                sql_datetime_sub=col.sql_datetime_sub,
                table_info=(
                    getattr(col, "table info") if hasattr(col, "table info") else None
                ),
                table_oid=(
                    getattr(col, "table oid") if hasattr(col, "table oid") else None
                ),
                table_name=col.table_name,
                table_owner=(
                    col.table_owner if hasattr(col, "table_owner") else col.table_schem
                ),
                table_qualifier=(
                    col.table_qualifier
                    if hasattr(col, "table_qualifier")
                    else col.table_cat
                ),
                type_name=col.type_name,
                typmod=(col.typmod if hasattr(col, "typmod") else None),
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


def pyodbc_table_exists(
    con: pyodbc.Connection,
    table_name: str,
    schema_name: typing.Optional[str],
) -> bool:
    with con.cursor() as cur:
        return bool(cur.tables(table=table_name, schema=schema_name).fetchone())
