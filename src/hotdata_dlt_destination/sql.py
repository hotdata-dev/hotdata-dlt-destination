from __future__ import annotations


def append_sql(*, target: str, staging: str) -> list[str]:
    return [
        f"CREATE TABLE IF NOT EXISTS {target} AS SELECT * FROM {staging} WHERE 1=0",
        f"INSERT INTO {target} SELECT * FROM {staging}",
    ]


def replace_from_staging_sql(*, target: str, staging: str) -> list[str]:
    return [
        f"DROP TABLE IF EXISTS {target}",
        f"CREATE TABLE {target} AS SELECT * FROM {staging}",
    ]


def merge_sql(*, target: str, staging: str) -> list[str]:
    return [
        f"CREATE TABLE IF NOT EXISTS {target} AS SELECT * FROM {staging} WHERE 1=0",
        f"DELETE FROM {target} t USING {staging} s "
        "WHERE t._hotdata_row_key = s._hotdata_row_key",
        f"INSERT INTO {target} SELECT * FROM {staging}",
    ]
