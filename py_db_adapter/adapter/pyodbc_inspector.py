"""
DB-API v2: https://www.python.org/dev/peps/pep-0249/

ref: see columns section of https://code.google.com/archive/p/pyodbc/wikis/Cursor.wiki

"""
import dataclasses
import pathlib
import pickle
import typing

import pyodbc

from py_db_adapter import domain

__all__ = ("inspect_table",)


def inspect_table(
    *,
    cur: pyodbc.Cursor,
    table_name: str,
    schema_name: typing.Optional[str],
    pk_cols: typing.Optional[typing.Set[str]] = None,
    include_cols: typing.Optional[typing.Set[str]] = None,
    cache_dir: typing.Optional[pathlib.Path] = None,
) -> domain.Table:
    if cache_dir:
        return pyodbc_inspect_table_and_cache(
            cur=cur,
            table_name=table_name,
            schema_name=schema_name,
            custom_pk_cols=pk_cols,
            include_cols=include_cols,
            cache_dir=cache_dir,
        )
    else:
        return pyodbc_inspect_table(
            cur=cur,
            table_name=table_name,
            schema_name=schema_name,
            custom_pk_cols=pk_cols,
            include_cols=include_cols,
        )


def pyodbc_inspect_table_and_cache(
    cache_dir: pathlib.Path,
    cur: pyodbc.Cursor,
    table_name: str,
    schema_name: typing.Optional[str] = None,
    custom_pk_cols: typing.Optional[typing.Set[str]] = None,
    include_cols: typing.Optional[typing.Set[str]] = None,
) -> domain.Table:
    fp = cache_dir / f"{schema_name}.{table_name}.p"
    if fp.exists():
        table = pickle.load(open(file=fp, mode="rb"))
    else:
        table = pyodbc_inspect_table(
            cur=cur,
            table_name=table_name,
            schema_name=schema_name,
            custom_pk_cols=custom_pk_cols,
            include_cols=include_cols,
        )
        pickle.dump(table, open(fp, "wb"))
    return table


def pyodbc_inspect_table(
    cur: pyodbc.Cursor,
    table_name: str,
    schema_name: typing.Optional[str] = None,
    custom_pk_cols: typing.Optional[typing.Set[str]] = None,
    include_cols: typing.Optional[typing.Set[str]] = None,
) -> domain.Table:
    if not pyodbc_table_exists(cur=cur, table_name=table_name, schema_name=schema_name):
        raise domain.exceptions.TableDoesNotExist(
            table_name=table_name, schema_name=schema_name
        )

    if include_cols:
        include_cols = {col.lower() for col in include_cols}

    if custom_pk_cols:
        custom_pk_cols = {col.lower() for col in custom_pk_cols}

    pyodbc_cols = _inspect_cols(cur=cur, table_name=table_name, schema_name=schema_name)
    domain_cols = []
    for col in pyodbc_cols:
        column_name = col.column_name.lower()
        if include_cols is None or column_name in include_cols:
            if col.domain_data_type == domain.DataType.Bool:
                domain_col: domain.Column = domain.BooleanColumn(
                    column_name=column_name,
                    nullable=col.nullable_flag,
                )
            elif col.domain_data_type == domain.DataType.Date:
                domain_col = domain.DateColumn(
                    column_name=column_name,
                    nullable=col.nullable_flag,
                )
            elif col.domain_data_type == domain.DataType.DateTime:
                domain_col = domain.DateTimeColumn(
                    column_name=column_name,
                    nullable=col.nullable_flag,
                )
            elif col.domain_data_type == domain.DataType.Decimal:
                domain_col = domain.DecimalColumn(
                    column_name=column_name,
                    nullable=col.nullable_flag,
                    precision=col.precision or 18,
                    scale=col.scale or 2,
                )
            elif col.domain_data_type == domain.DataType.Float:
                domain_col = domain.FloatColumn(
                    column_name=column_name,
                    nullable=col.nullable_flag,
                )
            elif col.domain_data_type == domain.DataType.Int:
                domain_col = domain.IntegerColumn(
                    autoincrement=col.autoincrement_flag,
                    column_name=column_name,
                    nullable=col.nullable_flag,
                )
            elif col.domain_data_type == domain.DataType.Text:
                domain_col = domain.TextColumn(
                    column_name=column_name,
                    nullable=col.nullable_flag,
                    max_length=col.length,
                )
            else:
                raise ValueError(
                    f"Unrecognized domain_data_type: {col.domain_data_type!r}"
                )

            domain_cols.append(domain_col)

    if custom_pk_cols:
        pk_col_names = custom_pk_cols
    else:
        pk_col_names = _inspect_pks(
            cur=cur, table_name=table_name, schema_name=schema_name
        )

    if not pk_col_names:
        raise domain.exceptions.MissingPrimaryKey(
            schema_name=schema_name, table_name=table_name
        )

    col_names = {col.column_name.lower() for col in domain_cols}
    missing_pk_col_names = {col for col in pk_col_names if col not in col_names}
    if missing_pk_col_names:
        raise domain.exceptions.InvalidCustomPrimaryKey(sorted(missing_pk_col_names))

    return domain.Table(
        schema_name=schema_name,
        table_name=table_name,
        columns=set(domain_cols),
        pk_cols=pk_col_names,
    )


@dataclasses.dataclass(frozen=True)
class PyodbcColumn:
    auto_increment: int
    column_name: str
    data_type: int
    field_type: int
    is_nullable: typing.Optional[int]
    length: typing.Optional[int]
    nullable: int
    precision: typing.Optional[int]
    scale: typing.Optional[int]
    type_name: str

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

    def __repr__(self) -> str:
        return repr(dataclasses.asdict(self))


def _inspect_cols(
    cur: pyodbc.Cursor, table_name: str, schema_name: typing.Optional[str]
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

    return [
        PyodbcColumn(
            auto_increment=get_autoincrement(col),
            column_name=col.column_name,
            data_type=col.data_type,
            field_type=get_field_type(col),
            is_nullable=handle_is_nullable(col.is_nullable),
            length=get_length(col),
            nullable=col.nullable,
            precision=get_precision(col),
            scale=get_scale(col),
            type_name=col.type_name,
        )
        for col in cur.columns(table_name, schema=schema_name)
    ]


def get_autoincrement(row: pyodbc.Row, /) -> int:
    if hasattr(row, "auto_increment"):
        return row.auto_increment
    return 0


def get_field_type(row: pyodbc.Row, /) -> int:
    if hasattr(row, "field_type"):
        return row.field_type
    elif hasattr(row, "user_data_type"):
        return row.user_data_type
    else:
        return row.ss_data_type  # sql server


def get_length(row: pyodbc.Row, /) -> typing.Optional[int]:
    if hasattr(row, "length"):
        return row.length
    return None


def get_precision(row: pyodbc.Row, /) -> typing.Optional[int]:
    if hasattr(row, "precision"):
        return row.precision
    return None


def get_scale(row: pyodbc.Row, /) -> typing.Optional[int]:
    if hasattr(row, "scale"):
        return row.scale
    return None


def _inspect_pks(
    cur: pyodbc.Cursor, table_name: str, schema_name: typing.Optional[str]
) -> typing.Set[str]:
    return {
        col.column_name.lower()
        for col in cur.primaryKeys(table_name, schema=schema_name)
    }


def pyodbc_table_exists(
    cur: pyodbc.Cursor,
    table_name: str,
    schema_name: typing.Optional[str],
) -> bool:
    return bool(cur.tables(table=table_name, schema=schema_name).fetchone())
