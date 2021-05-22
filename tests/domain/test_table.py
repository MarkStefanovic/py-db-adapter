import pytest

import py_db_adapter as pda
from py_db_adapter import domain


@pytest.fixture
def dummy_table() -> pda.Table:
    tbl = pda.Table(
        schema_name="dbo",
        table_name="test",
        columns=frozenset(
            {
                pda.Column(
                    column_name="test_id",
                    nullable=False,
                    data_type=pda.DataType.Int,
                    autoincrement=True,
                ),
                pda.Column(
                    column_name="test_name",
                    nullable=False,
                    data_type=pda.DataType.Text,
                    max_length=100,
                ),
                pda.Column(
                    column_name="last_run",
                    data_type=pda.DataType.DateTime,
                    nullable=False,
                ),
            }
        ),
        primary_key=pda.PrimaryKey(
            schema_name="dbo",
            table_name="test",
            columns=["test_id"],
        ),
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


def test_table_rejects_missing_pk_cols() -> None:
    with pytest.raises(domain.exceptions.TableMissingPrimaryKey):
        pda.Table(
            schema_name="test_schema",
            table_name="test_table",
            columns=frozenset(
                {
                    pda.Column(
                        column_name="first_name",
                        nullable=False,
                        data_type=pda.DataType.Text,
                        max_length=100,
                    ),
                    pda.Column(
                        column_name="last_name",
                        nullable=False,
                        data_type=pda.DataType.Text,
                        max_length=100,
                    ),
                }
            ),
            primary_key=pda.PrimaryKey(
                schema_name="test_schema",
                table_name="test_table",
                columns=[],
            ),
        )


def test_table_rejects_no_columns() -> None:
    with pytest.raises(domain.exceptions.TableHasNoColumns):
        pda.Table(
            schema_name="test_schema",
            table_name="test_table",
            columns=frozenset(),
            primary_key=pda.PrimaryKey(
                schema_name="test_schema",
                table_name="test_table",
                columns=["test_id"],
            ),
        )
