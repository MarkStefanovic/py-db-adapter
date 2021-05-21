__all__ = ("standardize_sql",)


def standardize_sql(sql: str) -> str:
    return (
        " ".join(sql.split())
        .replace("( ", "(")
        .replace(") )", "))")
        .replace(" )", ")")
        .replace(" , ", ", ")
        .strip()
    )
