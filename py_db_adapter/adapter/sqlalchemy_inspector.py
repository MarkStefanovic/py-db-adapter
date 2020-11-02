from __future__ import annotations

import datetime
import pathlib
import pickle
import typing

import sqlalchemy as sa
from sqlalchemy.engine import reflection

from py_db_adapter import domain

__all__ = (
    "inspect_table",
    "table_exists",
)


def inspect_table(
    engine: sa.engine.Engine,
    table_name: str,
    schema_name: typing.Optional[str] = None,
    inspector: typing.Optional[reflection.Inspector] = None,
) -> domain.Table:
    if inspector is None:
        inspector = sa.inspect(engine)

    domain_columns: typing.List[domain.Column] = []
    pks = inspector.get_pk_constraint(table_name=table_name, schema=schema_name)[
        "constrained_columns"
    ]
    for column in inspector.get_columns(table_name, schema=schema_name):
        dtype = column["type"]
        pk_flag = column["name"] in pks
        column_name = column["name"]
        nullable = column["nullable"]
        autoincrement = column["autoincrement"] is not None

        if dtype.python_type is bool:
            domain_col = domain.BooleanColumn(
                schema_name=schema_name,
                table_name=table_name,
                column_name=column_name,
                nullable=nullable,
                primary_key=pk_flag,
            )
        elif dtype.python_type is datetime.date:
            domain_col = domain.DateColumn(
                schema_name=schema_name,
                table_name=table_name,
                column_name=column_name,
                nullable=nullable,
                primary_key=pk_flag,
            )
        elif dtype.python_type is datetime.datetime:
            domain_col = domain.DateTimeColumn(
                schema_name=schema_name,
                table_name=table_name,
                column_name=column_name,
                nullable=nullable,
                primary_key=pk_flag,
            )
        elif hasattr(dtype, "scale") and dtype.scale is not None:
            domain_col = domain.DecimalColumn(
                schema_name=schema_name,
                table_name=table_name,
                column_name=column_name,
                nullable=nullable,
                primary_key=pk_flag,
                precision=dtype.precision,
                scale=dtype.scale,
            )
        elif dtype.python_type is float:
            domain_col = domain.FloatColumn(
                schema_name=schema_name,
                table_name=table_name,
                column_name=column_name,
                nullable=nullable,
                primary_key=pk_flag,
            )
        elif dtype.python_type is int:
            domain_col = domain.IntegerColumn(
                autoincrement=autoincrement,
                schema_name=schema_name,
                table_name=table_name,
                column_name=column_name,
                nullable=nullable,
                primary_key=pk_flag,
            )
        elif dtype.python_type is str or dtype.python_type is list:
            length = dtype.length if hasattr(dtype, "length") else None
            domain_col = domain.TextColumn(
                schema_name=schema_name,
                table_name=table_name,
                column_name=column_name,
                nullable=nullable,
                primary_key=pk_flag,
                max_length=length,
            )
        # elif dtype.python_type is list:
        #     data_type = domain.Text()
        # elif dtype is sa.types.Enum:
        #     data_type = domain.Text()
        else:
            raise ValueError(
                f"An error occurred while interpreting colum {column['name']!r}.  The data type "
                f"{dtype!r} does not correspond to a recognized DataType.  The "
                f"corresponding Python type is {dtype.python_type!r}."
            )

        domain_columns.append(domain_col)

    return domain.Table(
        schema_name=schema_name,
        table_name=table_name,
        columns=domain_columns,
    )


def table_exists(
    engine: sa.engine.Engine,
    schema_name: typing.Optional[str],
    table_name: str,
) -> bool:
    return engine.has_table(table_name=table_name, schema=schema_name)


def inspect_table_and_cache(
    cache_dir: pathlib.Path,
    engine: sa.engine.Engine,
    table_name: str,
    schema_name: typing.Optional[str] = None,
    inspector: typing.Optional[reflection.Inspector] = None,
):
    fp = cache_dir / f"{schema_name}.{table_name}.p"
    if fp.exists():
        table = pickle.load(open(file=fp, mode="rb"))
    else:
        table = inspect_table(
            engine=engine,
            table_name=table_name,
            schema_name=schema_name,
            inspector=inspector,
        )
        pickle.dump(table, open(fp, "wb"))
    return table
