from py_db_adapter.domain.rows import *


def test_as_dicts() -> None:
    items = list(zip("abcdefghij", range(10)))
    dummy_rows = Rows(
        column_names=["name", "age"],
        rows=items,
    )
    assert dummy_rows.as_dicts() == [
        {"name": "a", "age": 0},
        {"name": "b", "age": 1},
        {"name": "c", "age": 2},
        {"name": "d", "age": 3},
        {"name": "e", "age": 4},
        {"name": "f", "age": 5},
        {"name": "g", "age": 6},
        {"name": "h", "age": 7},
        {"name": "i", "age": 8},
        {"name": "j", "age": 9},
    ]


def test_update_with_transform() -> None:
    dummy_rows = Rows(
        column_names=["age", "name"],
        rows=[
            (100, "Mark"),
            (52, "Mandie"),
            (74, "Steve"),
        ],
    )
    updated_rows = dummy_rows.update_column_values(column_name="age", transform=lambda row: row["age"] + 1)
    assert updated_rows.as_tuples() == [("Mark", 100), ("Mandie", 53), ("Steve", 75)]


def test_update_with_static_value() -> None:
    dummy_rows = Rows(
        column_names=["name", "age"],
        rows=[
            ("Mark", 99),
            ("Mandie", 52),
            ("Steve", 74),
        ],
    )
    updated_rows = dummy_rows.update_column_values(column_name="age", static_value=99)
    assert updated_rows.as_tuples() == [("Mark", 99), ("Mandie", 99), ("Steve", 99)]
