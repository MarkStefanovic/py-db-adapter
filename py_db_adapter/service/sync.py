import dataclasses
import pathlib
import typing

import pyodbc

from py_db_adapter import adapter, domain
from py_db_adapter.service.copy_table import copy_table

__all__ = ("sync",)


logger = domain.root_logger.getChild("sync")


def sync(
    # fmt: off
    *,
    src_cur: pyodbc.Cursor,
    dest_cur: pyodbc.Cursor,
    src_db_adapter: domain.DbAdapter,
    dest_db_adapter: domain.DbAdapter,
    src_schema_name: typing.Optional[str],
    src_table_name: str,
    dest_schema_name: typing.Optional[str],
    dest_table_name: str,
    pk_cols: typing.Optional[typing.List[str]] = None,  # None = inspect to find out
    include_cols: typing.Optional[typing.Set[str]] = None,  # None = inspect to find out
    compare_cols: typing.Optional[typing.Set[str]] = None,  # None = compare on all common cols
    recreate: bool = False,
    cache_dir: typing.Optional[pathlib.Path] = None,
    skip_if_row_counts_match: bool = False,
    batch_size: int = 1000,
    # fmt: on
) -> domain.SyncResult:
    result = domain.SyncResult(
        src_schema_name=src_schema_name,
        src_table_name=src_table_name,
        dest_schema_name=dest_schema_name,
        dest_table_name=dest_table_name,
        added=0,
        deleted=0,
        updated=0,
        skipped=False,
        skipped_reason=None,
        error_message=None,
        traceback=None,
    )
    try:
        if src_db_adapter.fast_executemany_available:
            src_cur.fast_executemany = True

        if dest_db_adapter.fast_executemany_available:
            dest_cur.fast_executemany = True

        if skip_if_row_counts_match:
            dest_row_ct = dest_db_adapter.row_count(
                cur=dest_cur,
                table_name=dest_table_name,
                schema_name=dest_schema_name,
            )
            src_row_ct = src_db_adapter.row_count(
                cur=src_cur,
                table_name=src_table_name,
                schema_name=src_schema_name,
            )
            if src_row_ct == dest_row_ct:
                result = dataclasses.replace(
                    result, skipped=True, skipped_reason="rows already match"
                )
        if result.skipped:
            logger.info(f"Sync was skipped: {result.skipped_reason}")
        else:
            src_table = adapter.inspect_table(
                cur=src_cur,
                table_name=src_table_name,
                schema_name=src_schema_name,
                pk_cols=pk_cols,
                include_cols=include_cols,
                cache_dir=cache_dir,
            )
            dest_table, created = copy_table(
                src_cur=src_cur,
                dest_cur=dest_cur,
                dest_db_adapter=dest_db_adapter,
                src_table_name=src_table_name,
                src_schema_name=src_schema_name,
                dest_table_name=dest_table_name,
                dest_schema_name=dest_schema_name,
                recreate=recreate,
                include_cols=include_cols,
                pk_cols=pk_cols,
            )
            if created:
                if dest_schema_name:
                    dest_full_table_name = f"[{dest_schema_name}].[{dest_table_name}]"
                else:
                    dest_full_table_name = f"[{dest_table_name}]"
                logger.info(f"{dest_full_table_name} did not exist, so it was created.")

            if pk_cols is None:
                if src_table.primary_key.columns:
                    pks: typing.Set[str] = set(src_table.primary_key.columns)
                elif dest_table.primary_key.columns:
                    pks = set(dest_table.primary_key.columns)
                else:
                    raise domain.exceptions.MissingPrimaryKey(
                        schema_name=src_schema_name, table_name=src_table_name
                    )
            else:
                pks = set(pk_cols)

            if not include_cols:
                include_cols = src_table.column_names & dest_table.column_names

            if compare_cols is None:
                src_cols = src_table.non_pk_column_names
                dest_cols = dest_table.non_pk_column_names
                compare_cols = src_cols & dest_cols

            src_repo = domain.Repository(
                db=src_db_adapter,
                table=src_table,
                change_tracking_columns=compare_cols,
                batch_size=batch_size,
            )
            dest_repo = domain.Repository(
                db=dest_db_adapter,
                table=dest_table,
                change_tracking_columns=compare_cols,
                batch_size=batch_size,
            )

            dest_rows = dest_repo.keys(cur=dest_cur, additional_cols=compare_cols)

            if dest_rows.is_empty:
                logger.info(
                    f"{dest_table_name} is empty so the source rows will be fully loaded."
                )
                src_rows = src_repo.all(cur=src_cur, columns=include_cols)
                dest_repo.add(cur=dest_cur, rows=src_rows)
                result = dataclasses.replace(result, added=src_rows.row_count)
            else:
                src_keys = src_repo.keys(cur=src_cur, additional_cols=compare_cols)
                changes = domain.compare_rows(
                    src_rows=src_keys,
                    dest_rows=dest_rows,
                    key_cols=pks,
                    compare_cols=compare_cols,
                )

                if (
                    changes.rows_added.is_empty
                    and changes.rows_deleted.is_empty
                    and changes.rows_updated.is_empty
                ):
                    logger.info(
                        "Source and destination matched already, so there was no need to refresh."
                    )
                    result = dataclasses.replace(
                        result,
                        skipped=True,
                        skipped_reason="src and dest rows matched already",
                    )
                else:
                    if rows_added := changes.rows_added.row_count:
                        new_rows = src_repo.fetch_rows_by_primary_key_values(
                            cur=src_cur,
                            rows=changes.rows_added,
                            cols=include_cols,
                        )
                        dest_repo.add(cur=dest_cur, rows=new_rows)
                        logger.info(f"Added {rows_added} rows to [{src_table_name}].")
                    if rows_deleted := changes.rows_deleted.row_count:
                        dest_repo.delete(cur=dest_cur, rows=changes.rows_deleted)
                        logger.info(
                            f"Deleted {rows_deleted} rows from [{src_table_name}]."
                        )
                    if rows_updated := changes.rows_updated.row_count:
                        updated_rows = src_repo.fetch_rows_by_primary_key_values(
                            cur=src_cur,
                            rows=changes.rows_updated,
                            cols=include_cols,
                        )
                        dest_repo.update(
                            cur=dest_cur, rows=updated_rows, columns=include_cols
                        )
                        logger.info(
                            f"Updated {rows_updated} rows on [{src_table_name}]."
                        )
                    result = dataclasses.replace(
                        result,
                        added=rows_added,
                        deleted=rows_deleted,
                        updated=rows_updated,
                    )
    except Exception as e:
        tb = domain.exceptions.parse_traceback(e)
        result = dataclasses.replace(result, error_message=str(e), traceback=tb)
    finally:
        return result
