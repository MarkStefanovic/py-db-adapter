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


def test_as_lookup_table(dummy_rows: Rows) -> None:
    assert dummy_rows.as_lookup_table(key_columns=set("name")) == {
        ("a",): (0,),
        ("b",): (1,),
        ("c",): (2,),
        ("d",): (3,),
        ("e",): (4,),
        ("f",): (5,),
        ("g",): (6,),
        ("h",): (7,),
        ("i",): (8,),
        ("j",): (9,),
    }


def test_from_lookup_table(dummy_rows: Rows) -> None:
    lkp_table = dummy_rows.as_lookup_table(key_columns=set("name"))
    actual = Rows.from_lookup_table(
        lookup_table=lkp_table, key_columns=set("name"), value_columns=set("age")
    )
    assert actual == dummy_rows, f"Expected: {dummy_rows!s}\nActual: {actual!s}"
