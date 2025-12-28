from __future__ import annotations
from typing import Any, TypedDict

class HeartbeatState(TypedDict, total=False):
    run_id: str
    started_at: str

    # knobs
    limit: int
    enable_api: bool
    dry_run: bool

    # results per stage
    results: dict[str, Any]

    # errors per stage
    errors: dict[str, str]
