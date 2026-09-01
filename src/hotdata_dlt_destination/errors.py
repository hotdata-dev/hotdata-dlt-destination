from __future__ import annotations

from hotdata_framework.errors import (
    HotdataError,
    HotdataTerminalError,
    HotdataTransientError,
    classify_sdk_error,
)

# Backward-compatible alias kept for any code that referenced HotdataDestinationError.
HotdataDestinationError = HotdataError

# No typed error crosses the HTTP boundary for "that table isn't there", so it is
# matched on the server's phrasing. Shared by the SQL client (which maps it to
# DatabaseUndefinedRelation) and the job client (which reads it as "nothing
# stored yet"), so the two cannot drift apart.
UNDEFINED_RELATION_MARKERS = (
    "not found",
    "does not exist",
    "no such table",
    "no table named",
    "unknown table",
    "undefined table",
)

__all__ = [
    "UNDEFINED_RELATION_MARKERS",
    "HotdataDestinationError",
    "HotdataError",
    "HotdataTerminalError",
    "HotdataTransientError",
    "classify_sdk_error",
]
