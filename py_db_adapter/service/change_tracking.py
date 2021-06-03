import dataclasses
import datetime
import pathlib
import traceback
import typing

import pyodbc

from py_db_adapter import adapter, domain

__all__ = ("update_history_table",)

logger = domain.root_logger.getChild("change_tracking")


def update_history_table(
    *,
    src_cur: pyodbc.Cursor,
    dest_cur: pyodbc.Cursor,
    src_db_adapter: domain.DbAdapter,
    dest_db_adapter: domain.DbAdapter,
    src_schema_name: typing.Optional[str],
    src_table_name: str,
    compare_cols: typing.Optional[typing.Set[str]] = None,
    recreate: bool = False,
    cache_dir: typing.Optional[pathlib.Path] = None,
    pk_cols: typing.Optional[typing.Set[str]] = None,
    include_cols: typing.Optional[typing.Set[str]] = None,
) -> domain.ChangeTrackingResult:
    batch_utc_millis_since_epoch = int(
        (datetime.datetime.utcnow() - datetime.datetime(1970, 1, 1)).total_seconds()
        * 1000
    )
    result = domain.ChangeTrackingResult(
        new_rows=0,
        soft_deletes=0,
        new_row_versions=0,
        hist_table_created=False,
        pk_cols_inferred=False,
        include_cols_inferred=False,
        error_message=None,
        traceback=None,
        batch_utc_millis_since_epoch=batch_utc_millis_since_epoch,
    )

    # noinspection PyBroadException
    try:
        if pk_cols is None:
            result = dataclasses.replace(result, pk_cols_inferred=True)

        if include_cols is None:
            result = dataclasses.replace(result, include_cols_inferred=True)

        src_table = adapter.inspect_table(
            cur=src_cur,
            table_name=src_table_name,
            schema_name=src_schema_name,
            pk_cols=None if pk_cols is None else list(pk_cols),
            include_cols=include_cols,
            cache_dir=cache_dir,
        )
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
                result = dataclasses.replace(result, hist_table_created=True)

        if not hist_table_exists:
            logger.debug(f"Creating {hist_table.table_name}...")
            dest_db_adapter.create_table(cur=dest_cur, table=hist_table)

        hist_repo = domain.Repository(db=dest_db_adapter, table=hist_table)
        prior_state = get_prior_state(
            hist_cur=dest_cur,
            hist_db_adapter=dest_db_adapter,
            hist_table=hist_table,
        )
        current_state = get_current_state(
            cur=src_cur,
            db_adapter=src_db_adapter,
            table=src_table,
        )

        src_key_cols = set(src_table.primary_key.columns)
        changes = domain.compare_rows(
            key_cols=src_key_cols,
            compare_cols=compare_cols or src_table.non_pk_column_names,
            src_rows=current_state,
            dest_rows=prior_state,
        )
        if (
            changes.rows_added.is_empty
            and changes.rows_deleted.is_empty
            and changes.rows_updated.is_empty
        ):
            logger.info(
                "No changes have occurred on the source, so no row versions were added."
            )
        else:
            # fmt: off
            if rows_added := changes.rows_added.row_count:
                new_rows = (
                    changes.rows_added
                    .add_static_column(
                        column_name="valid_from",
                        value=batch_utc_millis_since_epoch,
                    )
                    .add_static_column(
                        column_name="valid_to",
                        value=datetime.datetime(9999, 12, 31),
                    )
                )
                hist_repo.add(cur=dest_cur, rows=new_rows)
                result = dataclasses.replace(result, new_rows=new_rows.row_count)
                logger.info(f"Added {rows_added} rows to [{hist_table.table_name}].")

            if rows_deleted := changes.rows_deleted.row_count:
                deleted_ids = {
                    frozenset(
                        (pk_col, row_dict[pk_col])
                        for pk_col in src_key_cols
                    )
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
                    .update_column_values(
                        column_name="valid_to",
                        static_value=batch_utc_millis_since_epoch,
                    )
                )
                # fmt: on
                hist_repo.update(cur=dest_cur, rows=soft_deletes)
                result = dataclasses.replace(result, soft_deletes=soft_deletes.row_count)
                logger.info(f"Soft deleted {rows_deleted} rows from [{hist_table.table_name}].")

            if rows_updated := changes.rows_updated.row_count:
                updated_ids = {
                    frozenset((pk_col, row_dict[pk_col]) for pk_col in src_key_cols)
                    for row_dict in changes.rows_updated.as_dicts()
                }
                old_versions = domain.Rows.from_dicts([
                    row_dict for row_dict in prior_state.as_dicts()
                    if frozenset(
                        (pk_col, row_dict[pk_col])
                        for pk_col in src_key_cols
                    ) in updated_ids
                ]).update_column_values(
                    column_name="valid_to",
                    static_value=batch_utc_millis_since_epoch - 1,
                )
                hist_repo.update(cur=dest_cur, rows=old_versions)

                new_versions = (
                    changes.rows_updated
                    .add_static_column(
                        column_name="valid_from",
                        value=batch_utc_millis_since_epoch,
                    )
                    .add_static_column(
                        column_name="valid_to",
                        value=domain.END_OF_TIME_MILLIS,
                    )
                )
                hist_repo.add(cur=dest_cur, rows=new_versions)
                result = dataclasses.replace(result, new_row_versions=new_versions.row_count)
                logger.info(f"Added {rows_updated} new row versions to [{hist_table.table_name}].")
            # fmt: on
    except Exception as e:
        tb = "".join(traceback.format_exception(None, e, e.__traceback__))
        result = dataclasses.replace(result, error_message=str(e), traceback=tb)
    finally:
        return result


def get_changes(
    *,
    hist_cur: pyodbc.Cursor,
    cur: pyodbc.Cursor,
    hist_db_adapter: domain.DbAdapter,
    db_adapter: domain.DbAdapter,
    hist_table: domain.Table,
    table: domain.Table,
    compare_cols: typing.Optional[typing.Set[str]] = None,
) -> domain.RowDiff:
    prior_state = get_prior_state(
        hist_cur=hist_cur,
        hist_db_adapter=hist_db_adapter,
        hist_table=hist_table,
    )
    current_state = get_current_state(
        cur=cur,
        db_adapter=db_adapter,
        table=table,
    )
    return domain.compare_rows(
        key_cols=set(table.primary_key.columns),
        compare_cols=compare_cols or table.non_pk_column_names,
        src_rows=current_state,
        dest_rows=prior_state,
    )


def get_current_state(
    *,
    cur: pyodbc.Cursor,
    db_adapter: domain.DbAdapter,
    table: domain.Table,
) -> domain.Rows:
    repo = domain.Repository(db=db_adapter, table=table)
    return repo.all(cur=cur, columns=table.column_names)


def get_prior_state(
    *,
    hist_cur: pyodbc.Cursor,
    hist_db_adapter: domain.DbAdapter,
    hist_table: domain.Table,
) -> domain.Rows:
    hist_repo = domain.Repository(db=hist_db_adapter, table=hist_table)
    return hist_repo.where(
        cur=hist_cur,
        predicate=domain.SqlPredicate(
            column_name="valid_to",
            operator=domain.SqlOperator.EQUALS,
            value=datetime.datetime(9999, 12, 31),
        ),
    )
