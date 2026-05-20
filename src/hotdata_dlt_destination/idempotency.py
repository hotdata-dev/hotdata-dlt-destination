from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from typing import Any


def stable_json_dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def compute_batch_key(table_name: str, items: Sequence[dict[str, Any]]) -> str:
    payload = {
        "table": table_name,
        "items": [stable_json_dumps(item) for item in items],
    }
    return hashlib.sha256(stable_json_dumps(payload).encode("utf-8")).hexdigest()


def compute_row_key(table_name: str, row: dict[str, Any]) -> str:
    payload = {
        "table": table_name,
        "row": stable_json_dumps(row),
    }
    return hashlib.sha256(stable_json_dumps(payload).encode("utf-8")).hexdigest()
