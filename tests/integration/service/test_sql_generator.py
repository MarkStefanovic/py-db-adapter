from py_db_adapter import adapter, service


def test_get_keys(sql_adapter: adapter.PostgreSQLTableAdapter) -> None:
    actual = service.sql_generator.get_keys(
        sql_adapter=sql_adapter, additional_cols=["date_added", "date_updated"]
    )
    expected = "SELECT employee_id, date_added, date_updated FROM hr.employee"
    assert actual == expected
