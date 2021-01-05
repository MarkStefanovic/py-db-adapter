from py_db_adapter.domain import row_diff, rows


def test_as_lookup_table() -> None:
    items = list(zip("abcdefghij", range(10)))
    dummy_rows = rows.Rows(
        column_names=["name", "age"],
        rows=items,
    )

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
