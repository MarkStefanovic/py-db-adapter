from __future__ import annotations

import abc
import pathlib
import pickle
import typing

from py_db_adapter.domain import db_connection, logger as domain_logger, table

__all__ = ("TableInspector",)

logger = domain_logger.root.getChild("TableInspector")


class TableInspector(abc.ABC):
    @abc.abstractmethod
    def inspect_table(
        self,
        *,
        db: db_connection,
        schema_name: typing.Optional[str],
        table_name: str,
        custom_pk_cols: typing.Set[str],
        include_cols: typing.Set[str],
    ) -> table.Table:
        raise NotImplementedError

    def inspect_table_and_cache(
        self,
        *,
        db: db_connection,
        schema_name: typing.Optional[str],
        table_name: str,
        custom_pk_cols: typing.Set[str],
        include_cols: typing.Set[str],
        cache_dir: pathlib.Path,
    ) -> table.Table:
        fp = cache_dir / f"{schema_name}.{table_name}.p"
        if fp.exists():
            tbl = pickle.load(open(file=fp, mode="rb"))
        else:
            tbl = self.inspect_table(
                db=db,
                schema_name=schema_name,
                table_name=table_name,
                custom_pk_cols=custom_pk_cols,
                include_cols=include_cols,
            )
            pickle.dump(tbl, open(fp, "wb"))
        return tbl
