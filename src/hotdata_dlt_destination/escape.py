"""SQL literal escaping for the instant database's engine.

dlt's Postgres escaper emits the *extended* string form, ``E'...'``, and this
engine's parser rejects it outright::

    sql parser error: Expected: an expression, found: E'nonexistent-hash'

So the destination cannot use ``escape_postgres_literal``, and the capability
must not point at it either: any dlt path that reads the capability would build
SQL the server refuses. Strings are standard-conforming here — a backslash is a
literal backslash and carries no escape meaning (verified against the API) — so
doubling the quote is the whole job.
"""

from __future__ import annotations

import json
from datetime import date, datetime, time
from typing import Any

__all__ = ["escape_hotdata_literal", "quote_hotdata_string"]


def quote_hotdata_string(value: str) -> str:
    """Quote `value` as a standard SQL string literal."""
    return "'" + value.replace("'", "''") + "'"


def escape_hotdata_literal(v: Any) -> Any:
    """A SQL literal for this engine.

    Same shape as dlt's Postgres escaper, without the ``E''`` prefix.
    """
    if isinstance(v, str):
        return quote_hotdata_string(v)
    if isinstance(v, (datetime, date, time)):
        return f"'{v.isoformat()}'"
    if isinstance(v, (list, dict)):
        return quote_hotdata_string(json.dumps(v))
    if isinstance(v, bytes):
        return f"'\\x{v.hex()}'"
    if v is None:
        return "NULL"
    return str(v)
