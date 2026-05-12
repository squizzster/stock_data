"""Small stderr JSONL progress helpers for long-running CLI commands."""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from typing import Any


def progress_payload(
    *,
    command: str,
    event_type: str,
    message: str,
    started_at: float,
    **fields: Any,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "command": command,
        "elapsed_seconds": round(max(time.monotonic() - started_at, 0), 3),
        "event_type": event_type,
        "message": message,
    }
    for key, value in fields.items():
        if value is None:
            continue
        if isinstance(value, Path):
            payload[key] = str(value)
        else:
            payload[key] = value
    return payload


def emit_stderr_progress(prefix: str, payload: dict[str, Any]) -> None:
    print(f"{prefix}{json.dumps(payload, sort_keys=True)}", file=sys.stderr, flush=True)


def emit_cli_progress(
    prefix: str,
    *,
    command: str,
    event_type: str,
    message: str,
    started_at: float,
    **fields: Any,
) -> None:
    emit_stderr_progress(
        prefix,
        progress_payload(
            command=command,
            event_type=event_type,
            message=message,
            started_at=started_at,
            **fields,
        ),
    )
