import pydantic
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
        "last_run",
        "test_history_id",
        "test_id",
        "test_name",
        "valid_from",
        "valid_to",
    }


def test_non_pk_column_names_method(dummy_table: pda.Table) -> None:
    assert dummy_table.non_pk_column_names == {"test_name", "last_run"}


def test_table_rejects_empty_pk_cols() -> None:
    with pytest.raises(pydantic.ValidationError):
        pda.Table(
            schema_name="test_schema",
            table_name="test_table",
            columns={
                pda.TextColumn(
                    column_name="first_name",
                    nullable=False,
                    max_length=100,
                ),
                pda.TextColumn(
                    column_name="last_name",
                    nullable=False,
                    max_length=100,
                ),
            },
            pk_cols=set(),
        )


def test_table_rejects_no_columns() -> None:
    with pytest.raises(pydantic.ValidationError):
        pda.Table(
            schema_name="test_schema",
            table_name="test_table",
            columns=set(),
            pk_cols={"test_id"},
        )
