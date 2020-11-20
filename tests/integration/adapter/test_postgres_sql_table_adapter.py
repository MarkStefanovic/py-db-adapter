import decimal

import pyodbc

from py_db_adapter import domain, adapter


def test_postgres_sql_table_adapter_columns_sql_mapping(
    pyodbc_postgres_con: pyodbc.Connection,
) -> None:
    table = adapter.pyodbc_inspect_table(
        con=pyodbc_postgres_con,
        table_name="employee",
        schema_name="hr",
    )
    sql_adapter = adapter.PostgreSQLAdapter(table=table)
    actual = {
        col._column.column_name: col.__class__.__name__
        for col in sql_adapter.column_adapters
    }
    expected = {
        "active": "PostgresBooleanColumnSqlAdapter",
        "date_added": "StandardDateTimeColumnSqlAdapter",
        "date_updated": "StandardDateTimeColumnSqlAdapter",
        "employee_dependents": "StandardIntegerColumnSqlAdapter",
        "employee_dob": "StandardDateColumnSqlAdapter",
        "employee_gender": "StandardTextColumnSqlAdapter",
        "employee_id": "StandardIntegerColumnSqlAdapter",
        "employee_middle_initial": "StandardTextColumnSqlAdapter",
        "employee_name": "StandardTextColumnSqlAdapter",
        "employee_performance_score": "StandardFloatColumnSqlAdapter",
        "employee_phone": "StandardIntegerColumnSqlAdapter",
        "employee_salary": "StandardDecimalColumnSqlAdapter",
        "quotes": "StandardTextColumnSqlAdapter",
    }
    assert actual == expected


def test_postgres_decimal_column_sql_adapter_literal() -> None:
    column = domain.DecimalColumn(
        schema_name="dummy",
        table_name="dummy_table",
        column_name="dummy",
        nullable=False,
        primary_key=False,
        precision=18,
        scale=2,
    )
    sql_adapter = adapter.StandardDecimalColumnSqlAdapter(column=column, wrapper=lambda o: f'"{o}"')
    actual = sql_adapter.literal(decimal.Decimal(1 / 12))
    expected = "0.08"
    assert actual == expected


def test_postgres_float_column_sql_adapter_literal() -> None:
    column = domain.FloatColumn(
        schema_name="dummy",
        table_name="dummy_table",
        column_name="dummy",
        nullable=False,
        primary_key=False,
    )
    sql_adapter = adapter.StandardFloatColumnSqlAdapter(
        column=column,
        wrapper=lambda o: f'"{o}"',
        max_decimal_places=4,
    )
    actual = sql_adapter.literal(1/12)
    expected = "0.0833"
    assert actual == expected


def test_create(employee_sql_table_adapter: adapter.SqlAdapter) -> None:
    assert employee_sql_table_adapter.definition == (
        "CREATE TABLE IF NOT EXISTS hr.employee (active BOOL NOT NULL, date_added "
        "TIMESTAMP NOT NULL, date_updated TIMESTAMP NULL, employee_dependents BIGINT NOT "
        "NULL, employee_dob DATE NOT NULL, employee_gender VARCHAR(255) NOT NULL, "
        "employee_id BIGINT NOT NULL, employee_middle_initial VARCHAR(2) NULL, "
        "employee_name VARCHAR(8190) NOT NULL, employee_performance_score FLOAT NOT "
        "NULL, employee_phone BIGINT NOT NULL, employee_salary DECIMAL(9, 2) NOT NULL, "
        "quotes VARCHAR(255) NULL, PRIMARY KEY (employee_id))"
    )


def test_drop(employee_sql_table_adapter: adapter.SqlAdapter) -> None:
    assert employee_sql_table_adapter.drop == "DROP TABLE IF EXISTS hr.employee"


def test_row_count(employee_sql_table_adapter: adapter.SqlAdapter) -> None:
    assert employee_sql_table_adapter.row_count == "SELECT COUNT(*) AS row_count FROM hr.employee"


def test_truncate(employee_sql_table_adapter: adapter.SqlAdapter) -> None:
    assert employee_sql_table_adapter.truncate == "TRUNCATE TABLE hr.employee"
