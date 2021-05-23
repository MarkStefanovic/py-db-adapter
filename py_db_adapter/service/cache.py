import pathlib
import typing

from py_db_adapter import domain

__all__ = ("clear", "clear_all")


def clear(
    *, cache_dir: pathlib.Path, schema_name: typing.Optional[str], table_name: str
) -> bool:
    schema_name = schema_name or "_"
    files_deleted = False
    if not cache_dir.exists():
        raise domain.exceptions.DirectoryDoesNotExit(folder=cache_dir)
    else:
        fps = list(cache_dir.glob(f"{schema_name}.{table_name}.*.*.p"))
        if fps:
            files_deleted = True
            for fp in fps:
                fp.unlink()
    return files_deleted


def clear_all(*, cache_dir: pathlib.Path) -> bool:
    files_deleted = False
    if not cache_dir.exists():
        raise domain.exceptions.DirectoryDoesNotExit(folder=cache_dir)
    else:
        fps = list(cache_dir.glob("*.p"))
        if fps:
            files_deleted = True
            for fp in fps:
                fp.unlink()
    return files_deleted
