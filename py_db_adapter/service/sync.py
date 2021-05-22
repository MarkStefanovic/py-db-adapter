import pathlib
import pickle
import typing

import pyodbc

from py_db_adapter import adapter, domain
from py_db_adapter.service.copy_table import copy_table

__all__ = ("sync",)


logger = domain.root.getChild("Datasource")


def sync(
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
    compare_cols: typing.Optional[
        typing.Set[str]
    ] = None,  # None = compare on all common cols
    recreate: bool = False,
    cache_dir: typing.Optional[pathlib.Path] = None,
    fast_executemany: bool = True,
) -> typing.Dict[str, int]:
    # TODO cur.fast_executemany = fast_executemany

    src_table = adapter.inspect_table(
        cur=src_cur,
        table_name=src_table_name,
        schema_name=src_schema_name,
        pk_cols=pk_cols,
        include_cols=include_cols,
        cache_dir=cache_dir,
    )
    dest_table = adapter.inspect_table(
        cur=dest_cur,
        table_name=dest_table_name,
        schema_name=dest_schema_name,
        pk_cols=pk_cols,
        include_cols=include_cols,
        cache_dir=cache_dir,
    )

    if pk_cols is None:
        if src_table.primary_key.columns:
            pk_cols = src_table.primary_key.columns
        elif dest_table.primary_key.columns:
            pk_cols = dest_table.primary_key.columns
        else:
            raise domain.exceptions.MissingPrimaryKey(
                schema_name=src_schema_name, table_name=src_table_name
            )

    if not include_cols:
        include_cols = src_table.column_names & dest_table.column_names

    dest_table, created = copy_table(
        cur=dest_cur,
        dest_db_adapter=dest_db_adapter,
        dest_schema_name=dest_schema_name,
        dest_table_name=dest_table_name,
        src_table=src_table,
        recreate=recreate,
    )

    if compare_cols is None:
        src_cols = src_table.non_pk_column_names
        dest_cols = dest_table.non_pk_column_names
        compare_cols = src_cols & dest_cols

    src_repo = domain.Repository(
        db=src_db_adapter,
        table=src_table,
        change_tracking_columns=compare_cols,
        batch_size=1_000,
    )
    dest_repo = domain.Repository(
        db=dest_db_adapter,
        table=dest_table,
        change_tracking_columns=compare_cols,
        batch_size=1_000,
    )

    if cache_dir:
        fp = cache_dir / f"{dest_table.table_name}.dest-keys.p"
        if fp.exists():
            dest_rows = pickle.load(open(file=fp, mode="rb"))
        else:
            dest_rows = dest_repo.keys(cur=dest_cur, additional_cols=compare_cols)
    else:
        dest_rows = dest_repo.keys(cur=dest_cur, additional_cols=compare_cols)

    if dest_rows.is_empty:
        logger.info(
            f"{dest_table_name} is empty so the source rows will be fully loaded."
        )
        src_rows = src_repo.all(cur=src_cur, columns=include_cols)
        dest_repo.add(cur=dest_cur, rows=src_rows)
        if cache_dir:
            dest_keys = src_rows.subset(set(pk_cols) | compare_cols)
            dump_dest_keys(
                cache_dir=cache_dir,
                table_name=dest_table_name,
                dest_keys=dest_keys,
            )
        return {
            "added": src_rows.row_count,
            "deleted": 0,
            "updated": 0,
        }
    else:
        src_keys = src_repo.keys(cur=src_cur, additional_cols=compare_cols)
        if cache_dir:
            dump_dest_keys(
                cache_dir=cache_dir,
                table_name=dest_table_name,
                dest_keys=src_keys,
            )
        changes = dest_rows.compare(
            rows=src_keys,
            key_cols=set(pk_cols),
            compare_cols=compare_cols,
            ignore_missing_key_cols=True,
            ignore_extra_key_cols=True,
        )

        if (
            changes.rows_added.is_empty
            and changes.rows_deleted.is_empty
            and changes.rows_updated.is_empty
        ):
            logger.info(
                "Source and destination matched already, so there was no need to refresh."
            )
            return {
                "added": 0,
                "deleted": 0,
                "updated": 0,
            }
        else:
            if rows_added := changes.rows_added.row_count:
                new_rows = src_repo.fetch_rows_by_primary_key_values(
                    cur=src_cur, rows=changes.rows_added, cols=src_table.column_names
                )
                dest_repo.add(cur=dest_cur, rows=new_rows)
                logger.info(f"Added {rows_added} rows to [{src_table_name}].")
            if rows_deleted := changes.rows_deleted.row_count:
                dest_repo.delete(cur=dest_cur, rows=changes.rows_deleted)
                logger.info(f"Deleted {rows_deleted} rows from [{src_table_name}].")
            if rows_updated := changes.rows_updated.row_count:
                updated_rows = src_repo.fetch_rows_by_primary_key_values(
                    cur=src_cur, rows=changes.rows_updated, cols=src_table.column_names
                )
                dest_repo.update(cur=dest_cur, rows=updated_rows)
                logger.info(f"Updated {rows_updated} rows on [{src_table_name}].")
            return {
                "added": rows_added,
                "deleted": rows_deleted,
                "updated": rows_updated,
            }


def clear_cache(*, cache_dir: pathlib.Path, table_name: str) -> None:
    if cache_dir:
        fp = cache_dir / f"{table_name}.dest-keys.p"
        if fp.exists():
            fp.unlink()


def dump_dest_keys(
    cache_dir: pathlib.Path, table_name: str, dest_keys: domain.Rows
) -> None:
    fp = cache_dir / f"{table_name}.dest-keys.p"
    pickle.dump(dest_keys, open(fp, "wb"))
