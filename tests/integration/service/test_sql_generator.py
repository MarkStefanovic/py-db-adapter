from py_db_adapter import adapter, service


def test_get_keys(employee_sql_table_adapter) -> None:
    actual = service.sql_generator.get_keys(
        sql_adapter=employee_sql_table_adapter, additional_cols=["date_added", "date_updated"]
    )
    expected = "SELECT employee_id, date_added, date_updated FROM hr.employee"
    assert actual == expected
