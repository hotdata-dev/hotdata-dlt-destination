from __future__ import annotations

from hotdata_framework.errors import (
    HotdataError,
    HotdataTerminalError,
    HotdataTransientError,
    classify_sdk_error,
)

# Backward-compatible alias kept for any code that referenced HotdataDestinationError.
HotdataDestinationError = HotdataError

__all__ = [
    "HotdataDestinationError",
    "HotdataError",
    "HotdataTerminalError",
    "HotdataTransientError",
    "classify_sdk_error",
]
