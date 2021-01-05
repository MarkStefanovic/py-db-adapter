import pytest

import py_db_adapter as pda


@pytest.fixture
def dummy_table() -> pda.Table:
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
    assert len(tbl.column_names) == 3
    return tbl


def test_to_history_table(dummy_table: pda.Table) -> None:
    history_table = dummy_table.as_history_table()
    assert history_table.column_names == {
        "valid_from",
        "valid_to",
        "test_id",
        "last_run",
        "version",
        "test_name",
    }


def test_non_pk_column_names_method(dummy_table: pda.Table) -> None:
    assert dummy_table.non_pk_column_names == {"test_name", "last_run"}
