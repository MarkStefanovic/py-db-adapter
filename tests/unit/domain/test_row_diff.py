import pytest

from py_db_adapter.domain import row_diff, rows


@pytest.fixture
def dummy_rows() -> rows.Rows:
    items = list(zip("abcdefghij", range(10)))
    return rows.Rows(
        column_names=["name", "age"],
        rows=items,
    )


def test_as_lookup_table(dummy_rows: rows.Rows) -> None:
    assert row_diff.rows_to_lookup_table(
        rs=dummy_rows, key_columns={"name"}, value_columns={"age"}
    ) == {
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
