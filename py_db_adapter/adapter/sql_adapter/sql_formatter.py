def standardize_sql(sql: str) -> str:
    return (
        " ".join(sql.split())
        .replace("( ", "(")
        .replace(") )", "))")
        .replace(" , ", ", ")
        .strip()
    )
