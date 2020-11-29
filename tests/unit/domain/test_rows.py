import pytest

from py_db_adapter.domain.rows import *


@pytest.fixture
def dummy_rows() -> Rows:
    items = list(zip("abcdefghij", range(10)))
    return Rows(
        column_names=["name", "age"],
        rows=items,
    )


def test_as_dicts(dummy_rows: Rows) -> None:
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
