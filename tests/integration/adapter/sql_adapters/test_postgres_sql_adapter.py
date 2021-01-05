import decimal

import pyodbc

import py_db_adapter as pda


def test_postgres_decimal_column_sql_adapter_literal() -> None:
    column = pda.DecimalColumn(
        column_name="dummy",
        nullable=False,
        precision=18,
        scale=2,
        autoincrement=False,
    )
    sql_adapter = pda.StandardDecimalColumnSqlAdapter(
        col=column, wrapper=lambda o: f'"{o}"'
    )
    actual = sql_adapter.literal(decimal.Decimal(1 / 12))
    expected = "0.08"
    assert actual == expected


def test_postgres_float_column_sql_adapter_literal() -> None:
    column = pda.FloatColumn(
        column_name="dummy",
        nullable=False,
        autoincrement=False,
    )
    sql_adapter = pda.StandardFloatColumnSqlAdapter(
        col=column,
        wrapper=lambda o: f'"{o}"',
        max_decimal_places=4,
    )
    actual = sql_adapter.literal(1 / 12)
    expected = "0.0833"
    assert actual == expected


def test_create_table_sql(postgres_pyodbc_db_uri: str) -> None:
    sql_adapter = pda.PostgreSQLAdapter()
    with pyodbc.connect(postgres_pyodbc_db_uri) as con:
        table = pda.pyodbc_inspect_table(
            con=con,
            table_name="employee",
            schema_name="hr",
        )
    actual = sql_adapter.definition(table)
    expected = (
        "CREATE TABLE hr.employee (active BOOL NOT NULL, date_added TIMESTAMP NOT NULL, date_updated TIMESTAMP NULL, "
        "employee_dependents BIGINT NOT NULL, employee_dob DATE NOT NULL, employee_gender VARCHAR(255) NOT NULL, "
        "employee_id BIGINT NOT NULL, employee_middle_initial VARCHAR(2) NULL, employee_name VARCHAR(8190) NOT NULL, "
        "employee_performance_score FLOAT NOT NULL, employee_phone BIGINT NOT NULL, "
        "employee_salary DECIMAL(9, 2) NOT NULL, quotes VARCHAR(255) NULL, PRIMARY KEY (employee_id))"
    )
    assert actual == expected, f"{actual=}/n{expected=}"


def test_drop_table_sql() -> None:
    sql_adapter = pda.PostgreSQLAdapter()
    assert (
        sql_adapter.drop(schema_name="hr", table_name="employee")
        == "DROP TABLE hr.employee"
    )


def test_row_count_sql() -> None:
    sql_adapter = pda.PostgreSQLAdapter()
    assert (
        sql_adapter.row_count(schema_name="hr", table_name="employee")
        == "SELECT COUNT(*) AS row_count FROM hr.employee"
    )


def test_truncate_table_sql() -> None:
    sql_adapter = pda.PostgreSQLAdapter()
    assert (
        sql_adapter.truncate(schema_name="hr", table_name="employee")
        == "TRUNCATE TABLE hr.employee"
    )


def test_select_where_method() -> None:
    schema_name = "dbo"
    table_name = "test"
    tbl = pda.Table(
        schema_name=schema_name,
        table_name=table_name,
        columns={
            pda.IntegerColumn(
                column_name="test_id",
                nullable=False,
                autoincrement=True,
            ),
            pda.TextColumn(
                column_name="test_name",
                nullable=False,
                max_length=100,
            ),
            pda.DateTimeColumn(
                column_name="last_run",
                nullable=False,
            ),
        },
        pk_cols={"test_id"},
    )
    sql_adapter = pda.PostgreSQLAdapter()
    sql = sql_adapter.select_where(
        table=tbl,
        predicate=pda.SqlPredicate(
            column_name="test_name",
            operator=pda.SqlOperator.EQUALS,
            value="test_id",
        ),
    )
    # fmt: off
    assert sql == "SELECT last_run,test_name,test_id FROM dbo.test WHERE test_name = 'test_id'"
    # fmt: on
