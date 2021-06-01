import datetime
import pathlib
import pickle
import typing

import pyodbc

import py_db_adapter.domain.row_diff
import py_db_adapter.domain.rows
from py_db_adapter import domain

__all__ = ("update_history_table",)

logger = domain.root_logger.getChild("change_tracking")


def update_history_table(
    *,
    src_cur: pyodbc.Cursor,
    dest_cur: pyodbc.Cursor,
    src_db_adapter: domain.DbAdapter,
    dest_db_adapter: domain.DbAdapter,
    src_table: domain.Table,
    compare_cols: typing.Optional[typing.Set[str]] = None,
    recreate: bool = False,
    cache_dir: typing.Optional[pathlib.Path] = None,
) -> typing.Dict[str, int]:
    ts = datetime.datetime.now()

    hist_table = src_table.as_history_table()

    hist_table_exists = dest_db_adapter.table_exists(
        cur=dest_cur,
        schema_name=hist_table.schema_name,
        table_name=hist_table.table_name,
    )

    if recreate:
        logger.debug(f"Recreating {hist_table.table_name}...")
        if hist_table_exists:
            dest_db_adapter.drop_table(
                cur=dest_cur,
                table_name=hist_table.table_name,
                schema_name=hist_table.schema_name,
            )

    if not hist_table_exists:
        logger.debug(f"Creating {hist_table.table_name}...")
        dest_db_adapter.create_table(cur=dest_cur, table=hist_table)

    hist_repo = domain.Repository(db=dest_db_adapter, table=hist_table)
    prior_state = get_prior_state(
        hist_cur=dest_cur,
        hist_db_adapter=dest_db_adapter,
        hist_table=hist_table,
        cache_dir=cache_dir,
    )
    current_state = get_current_state(
        cur=src_cur,
        db_adapter=src_db_adapter,
        table=src_table,
        cache_dir=cache_dir,
    )

    src_key_cols = set(src_table.primary_key.columns)
    changes = py_db_adapter.domain.compare_rows(
        key_cols=src_key_cols,
        compare_cols=compare_cols or src_table.non_pk_column_names,
        src_rows=current_state,
        dest_rows=prior_state,
        ignore_missing_key_cols=False,
        ignore_extra_key_cols=True,
    )
    if (
        changes.rows_added.is_empty
        and changes.rows_deleted.is_empty
        and changes.rows_updated.is_empty
    ):
        logger.info(
            "No changes have occurred on the source, so no row versions were added."
        )
        return {"added": 0, "deleted": 0, "updated": 0}
    else:
        # fmt: off
        if rows_added := changes.rows_added.row_count:
            new_rows = (
                changes.rows_added
                .add_static_column(column_name="valid_from", value=ts)
                .add_static_column(column_name="valid_to", value=datetime.datetime(9999, 12, 31))
            )
            hist_repo.add(cur=dest_cur, rows=new_rows)
            logger.info(f"Added {rows_added} rows to [{hist_table.table_name}].")
        if rows_deleted := changes.rows_deleted.row_count:
            deleted_ids = {
                frozenset((pk_col, row_dict[pk_col]) for pk_col in src_key_cols)
                for row_dict in changes.rows_deleted.as_dicts()
            }
            # fmt: off
            soft_deletes = (
                domain.Rows.from_dicts([
                    row_dict for row_dict in prior_state.as_dicts()
                    if frozenset(
                        (pk_col, row_dict[pk_col])
                        for pk_col in src_key_cols
                    ) in deleted_ids
                ])
                .update_column_values(column_name="valid_to", static_value=ts)
            )
            # fmt: on
            hist_repo.update(cur=dest_cur, rows=soft_deletes)
            logger.info(f"Soft deleted {rows_deleted} rows from [{hist_table.table_name}].")
        if rows_updated := changes.rows_updated.row_count:
            updated_ids = {
                frozenset((pk_col, row_dict[pk_col]) for pk_col in src_key_cols)
                for row_dict in changes.rows_updated.as_dicts()
            }
            old_versions = domain.Rows.from_dicts([
                row_dict for row_dict in prior_state.as_dicts()
                if frozenset((pk_col, row_dict[pk_col]) for pk_col in src_key_cols) in updated_ids
            ]).update_column_values(
                column_name="valid_to",
                static_value=ts - datetime.timedelta(microseconds=1),
            )
            hist_repo.update(cur=dest_cur, rows=old_versions)

            new_versions = (
                changes.rows_updated
                .add_static_column(
                    column_name="valid_from",
                    value=ts,
                )
                .add_static_column(
                    column_name="valid_to",
                    value=datetime.datetime(9999, 12, 31)
                )
            )
            hist_repo.add(cur=dest_cur, rows=new_versions)
            logger.info(f"Added {rows_updated} new row versions to [{hist_table.table_name}].")
        # fmt: on
        return {
            "new rows": rows_added,
            "soft-deletes": rows_deleted,
            "new row versions": rows_updated,
        }


def get_changes(
    *,
    hist_cur: pyodbc.Cursor,
    cur: pyodbc.Cursor,
    hist_db_adapter: domain.DbAdapter,
    db_adapter: domain.DbAdapter,
    hist_table: domain.Table,
    table: domain.Table,
    compare_cols: typing.Optional[typing.Set[str]] = None,
    cache_dir: typing.Optional[pathlib.Path] = None,
) -> py_db_adapter.domain.row_diff.RowDiff:
    prior_state = get_prior_state(
        hist_cur=hist_cur,
        hist_db_adapter=hist_db_adapter,
        hist_table=hist_table,
        cache_dir=cache_dir,
    )
    current_state = get_current_state(
        cur=cur,
        db_adapter=db_adapter,
        table=table,
        cache_dir=cache_dir,
    )
    if cache_dir:
        save_state(
            cache_dir=cache_dir,
            schema_name=table.schema_name,
            table_name=table.table_name,
            current_rows=current_state,
        )
    return py_db_adapter.domain.compare_rows(
        key_cols=set(table.primary_key.columns),
        compare_cols=compare_cols or table.non_pk_column_names,
        src_rows=current_state,
        dest_rows=prior_state,
        ignore_missing_key_cols=False,
        ignore_extra_key_cols=True,
    )


def get_current_state(
    *,
    cur: pyodbc.Cursor,
    db_adapter: domain.DbAdapter,
    table: domain.Table,
    cache_dir: typing.Optional[pathlib.Path] = None,
) -> domain.Rows:
    repo = domain.Repository(db=db_adapter, table=table)
    if cache_dir is None:
        return repo.all(cur=cur, columns=table.column_names)
    else:
        schema_name = table.schema_name or "_"
        fp = cache_dir / f"{schema_name}.{table.table_name}.state.p"
        if fp.exists():
            with fp.open("rb") as fh:
                return pickle.load(fh)
        else:
            return repo.all(cur=cur, columns=table.column_names)


def get_prior_state(
    *,
    hist_cur: pyodbc.Cursor,
    hist_db_adapter: domain.DbAdapter,
    hist_table: domain.Table,
    cache_dir: typing.Optional[pathlib.Path] = None,
) -> domain.Rows:
    hist_repo = domain.Repository(db=hist_db_adapter, table=hist_table)
    if cache_dir is None:
        return hist_repo.where(
            cur=hist_cur,
            predicate=domain.SqlPredicate(
                column_name="valid_to",
                operator=domain.SqlOperator.EQUALS,
                value=datetime.datetime(9999, 12, 31),
            ),
        )
    else:
        schema_name = hist_table.schema_name or "_"
        fp = cache_dir / f"{schema_name}.{hist_table.table_name}.p"
        if fp.exists():
            with fp.open("rb") as fh:
                return pickle.load(fh)
        else:
            return hist_repo.where(
                cur=hist_cur,
                predicate=domain.SqlPredicate(
                    column_name="valid_to",
                    operator=domain.SqlOperator.EQUALS,
                    value=datetime.datetime(9999, 12, 31),
                ),
            )


def save_state(
    *,
    cache_dir: pathlib.Path,
    schema_name: typing.Optional[str],
    table_name: str,
    current_rows: domain.Rows,
) -> None:
    schema_name = schema_name or "_"
    fp = cache_dir / f"{schema_name}.{table_name}.state.p"
    with fp.open("wb") as fh:
        pickle.dump(current_rows, fh)
