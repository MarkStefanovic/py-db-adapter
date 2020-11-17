import typing

from py_db_adapter.domain import rows


def compare_rows(
    *,
    key_cols: typing.Set[str],
    src_rows: rows.Rows,
    dest_rows: rows.Rows,
) -> typing.Dict[str, rows.Rows]:
    common_cols = sorted(
        set(src_rows.column_names).intersection(set(dest_rows.column_names))
    )
    compare_cols = {col for col in common_cols if col not in key_cols}
    key_cols = {col for col in key_cols if col in common_cols}
    src_hashes = src_rows.as_lookup_table(
        key_columns=key_cols, value_columns=compare_cols
    )
    dest_hashes = dest_rows.as_lookup_table(
        key_columns=key_cols, value_columns=compare_cols
    )
    src_key_set = set(src_hashes.keys())
    dest_key_set = set(dest_hashes.keys())
    added = {k: src_hashes[k] for k in (src_key_set - dest_key_set)}
    deleted = {k: src_hashes.get(k, tuple()) for k in (dest_key_set - src_key_set)}
    updates = {
        k: src_hashes.get(k, tuple())
        for k in src_key_set
        if k not in added
        and k not in deleted
        and src_hashes.get(k, tuple()) != dest_hashes.get(k, tuple())
    }
    return {
        "added": rows.Rows.from_lookup_table(
            lookup_table=added,
            key_columns=key_cols,
            value_columns=compare_cols,
        ),
        "deleted": rows.Rows.from_lookup_table(
            lookup_table=deleted,
            key_columns=key_cols,
            value_columns=compare_cols,
        ),
        "updated": rows.Rows.from_lookup_table(
            lookup_table=updates,
            key_columns=key_cols,
            value_columns=compare_cols,
        ),
    }
