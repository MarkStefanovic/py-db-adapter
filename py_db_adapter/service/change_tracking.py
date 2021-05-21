import py_db_adapter.domain.rows


def update_history_table(
    *,
    compare_cols: typing.Optional[typing.Set[str]] = None,
    recreate: bool = False,
) -> typing.Dict[str, int]:
    ts = datetime.datetime.now()

    live_table = self._table
    hist_table = live_table.as_history_table()
    ds = self.copy(update={"table_name": hist_table.table_name})

    hist_table_exists = ds.db.table_exists(
        schema_name=hist_table.schema_name, table_name=hist_table.table_name
    )

    if recreate:
        logger.debug(f"Recreating {hist_table.table_name}...")
        if hist_table_exists:
            ds.db.drop_table(
                table_name=hist_table.table_name, schema_name=hist_table.schema_name
            )

    if not hist_table_exists:
        logger.debug(f"Creating {hist_table.table_name}...")
        ds.db.create_table(hist_table)

    hist_repo = ds._create_repo(hist_table)

    prior_state = hist_repo.where(
        predicate=domain.SqlPredicate(
            column_name="valid_to",
            operator=domain.SqlOperator.EQUALS,
            value=datetime.datetime(9999, 12, 31),
        )
    )
    live_repo = self._create_repo(live_table)
    current_state = live_repo.all(live_table.column_names)
    changes = py_db_adapter.domain.rows.RowDiff(
        key_cols=live_table.pk_cols,
        compare_cols=compare_cols or live_table.non_pk_column_names,
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
            hist_repo.add(new_rows)
            logger.info(f"Added {rows_added} rows to [{hist_table.table_name}].")
        if rows_deleted := changes.rows_deleted.row_count:
            deleted_ids = {
                frozenset((pk_col, row_dict[pk_col]) for pk_col in live_table.pk_cols)
                for row_dict in changes.rows_deleted.as_dicts()
            }
            soft_deletes = (
                domain.Rows.from_dicts([
                    row_dict for row_dict in prior_state.as_dicts()
                    if frozenset((pk_col, row_dict[pk_col]) for pk_col in live_table.pk_cols) in deleted_ids
                ])
                    .update_column_values(column_name="valid_to", static_value=ts)
            )
            hist_repo.update(rows=soft_deletes)
            logger.info(f"Soft deleted {rows_deleted} rows from [{hist_table.table_name}].")
        if rows_updated := changes.rows_updated.row_count:
            updated_ids = {
                frozenset((pk_col, row_dict[pk_col]) for pk_col in live_table.pk_cols)
                for row_dict in changes.rows_updated.as_dicts()
            }
            old_versions = domain.Rows.from_dicts([
                row_dict for row_dict in prior_state.as_dicts()
                if frozenset((pk_col, row_dict[pk_col]) for pk_col in live_table.pk_cols) in updated_ids
            ]).update_column_values(
                column_name="valid_to",
                static_value=ts - datetime.timedelta(microseconds=1),
            )
            hist_repo.update(old_versions)

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
            hist_repo.add(new_versions)
            logger.info(f"Added {rows_updated} new row versions to [{hist_table.table_name}].")
        ds.db.commit()
        # fmt: on
        return {
            "new rows": rows_added,
            "soft-deletes": rows_deleted,
            "new row versions": rows_updated,
        }