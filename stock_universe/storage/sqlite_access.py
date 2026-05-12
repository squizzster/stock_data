"""Central SQLite access helpers for stock-universe processes."""

from __future__ import annotations

import re
import sqlite3
import time
import urllib.parse
from dataclasses import dataclass
from logging import Logger, getLogger
from pathlib import Path
from typing import Any, Callable, Iterable


_SQL_NO_PARAMETERS: tuple[Any, ...] = ()
_SQL_OPERATION_RE = re.compile(
    r"^\s*(?:--[^\n]*\n\s*|/\*.*?\*/\s*)*([A-Za-z]+)", re.DOTALL
)
DEFAULT_SQLITE_BUSY_TIMEOUT_MS = 60_000


@dataclass(frozen=True)
class SqlEvent:
    phase: str
    label: str
    operation: str
    sql: str
    duration_seconds: float | None = None
    error_type: str = ""
    error_message: str = ""


SqlEventHandler = Callable[[SqlEvent], None]
_sql_event_handler: SqlEventHandler | None = None


class StockUniverseSQLiteCursor(sqlite3.Cursor):
    """Cursor subclass that routes SQL execution through this module."""

    def execute(
        self, sql: str, parameters: Any = _SQL_NO_PARAMETERS, /
    ) -> sqlite3.Cursor:
        return sql_cursor_execute(self, "cursor.execute", sql, parameters)

    def executemany(
        self, sql: str, seq_of_parameters: Iterable[Any], /
    ) -> sqlite3.Cursor:
        return sql_cursor_executemany(
            self, "cursor.executemany", sql, seq_of_parameters
        )

    def executescript(self, sql_script: str, /) -> sqlite3.Cursor:
        return sql_cursor_executescript(self, "cursor.executescript", sql_script)


class StockUniverseSQLiteConnection(sqlite3.Connection):
    """Connection subclass that makes conn.execute(...) an interception point."""

    def execute(
        self, sql: str, parameters: Any = _SQL_NO_PARAMETERS, /
    ) -> sqlite3.Cursor:
        return sql_execute(self, "connection.execute", sql, parameters)

    def executemany(
        self, sql: str, seq_of_parameters: Iterable[Any], /
    ) -> sqlite3.Cursor:
        return sql_executemany(self, "connection.executemany", sql, seq_of_parameters)

    def executescript(self, sql_script: str, /) -> sqlite3.Cursor:
        return sql_executescript(self, "connection.executescript", sql_script)

    def cursor(self, factory: type[sqlite3.Cursor] | None = None, /) -> sqlite3.Cursor:
        return super().cursor(factory or StockUniverseSQLiteCursor)


def set_sql_event_handler(handler: SqlEventHandler | None) -> None:
    """Install a process-local SQL event handler for managed connections."""

    global _sql_event_handler
    _sql_event_handler = handler


def install_slow_sql_logger(
    *,
    threshold_seconds: float,
    logger: Logger | None = None,
) -> None:
    """Log managed SQL calls whose measured duration crosses the threshold."""

    log = logger or getLogger("stock_universe.sql")

    def _handler(event: SqlEvent) -> None:
        if event.phase not in {"after", "error"}:
            return
        if event.duration_seconds is None or event.duration_seconds < threshold_seconds:
            return
        suffix = (
            f" error={event.error_type}: {event.error_message}"
            if event.error_type
            else ""
        )
        log.warning(
            "slow sqlite %s label=%s duration_ms=%.3f sql=%r%s",
            event.operation,
            event.label,
            event.duration_seconds * 1000,
            event.sql,
            suffix,
        )

    set_sql_event_handler(_handler)


def connect_sqlite_database(
    database: str | Path,
    *,
    uri: bool = False,
    timeout: float = DEFAULT_SQLITE_BUSY_TIMEOUT_MS / 1000,
    row_factory: Any = sqlite3.Row,
) -> sqlite3.Connection:
    conn = sqlite3.connect(
        database,
        uri=uri,
        timeout=timeout,
        factory=StockUniverseSQLiteConnection,
    )
    if row_factory is not None:
        conn.row_factory = row_factory
    conn.execute(f"PRAGMA busy_timeout = {DEFAULT_SQLITE_BUSY_TIMEOUT_MS}")
    return conn


def connect_readonly_sqlite(
    path: str | Path, *, row_factory: Any = sqlite3.Row
) -> sqlite3.Connection:
    conn = connect_sqlite_database(
        readonly_db_uri(Path(path)), uri=True, row_factory=row_factory
    )
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA query_only = ON")
    return conn


def readonly_db_uri(path: Path) -> str:
    return f"file:{urllib.parse.quote(str(path.resolve()), safe='/')}?mode=ro"


def sql_select_all(
    conn: sqlite3.Connection,
    label: str,
    sql: str,
    parameters: Any = _SQL_NO_PARAMETERS,
) -> list[sqlite3.Row]:
    _emit_sql_event("before", label, sql)
    started = time.perf_counter()
    try:
        rows = sqlite3.Connection.execute(conn, sql, parameters).fetchall()
    except sqlite3.Error as exc:
        _emit_sql_event(
            "error", label, sql, exc, duration_seconds=time.perf_counter() - started
        )
        raise
    _emit_sql_event("after", label, sql, duration_seconds=time.perf_counter() - started)
    return rows


def sql_select_one(
    conn: sqlite3.Connection,
    label: str,
    sql: str,
    parameters: Any = _SQL_NO_PARAMETERS,
) -> sqlite3.Row | None:
    _emit_sql_event("before", label, sql)
    started = time.perf_counter()
    try:
        row = sqlite3.Connection.execute(conn, sql, parameters).fetchone()
    except sqlite3.Error as exc:
        _emit_sql_event(
            "error", label, sql, exc, duration_seconds=time.perf_counter() - started
        )
        raise
    _emit_sql_event("after", label, sql, duration_seconds=time.perf_counter() - started)
    return row


def sql_execute(
    conn: sqlite3.Connection,
    label: str,
    sql: str,
    parameters: Any = _SQL_NO_PARAMETERS,
) -> sqlite3.Cursor:
    _emit_sql_event("before", label, sql)
    started = time.perf_counter()
    try:
        cursor = sqlite3.Connection.execute(conn, sql, parameters)
    except sqlite3.Error as exc:
        _emit_sql_event(
            "error", label, sql, exc, duration_seconds=time.perf_counter() - started
        )
        raise
    _emit_sql_event("after", label, sql, duration_seconds=time.perf_counter() - started)
    return cursor


def sql_executemany(
    conn: sqlite3.Connection,
    label: str,
    sql: str,
    seq_of_parameters: Iterable[Any],
) -> sqlite3.Cursor:
    _emit_sql_event("before", label, sql)
    started = time.perf_counter()
    try:
        cursor = sqlite3.Connection.executemany(conn, sql, seq_of_parameters)
    except sqlite3.Error as exc:
        _emit_sql_event(
            "error", label, sql, exc, duration_seconds=time.perf_counter() - started
        )
        raise
    _emit_sql_event("after", label, sql, duration_seconds=time.perf_counter() - started)
    return cursor


def sql_executescript(
    conn: sqlite3.Connection, label: str, sql_script: str
) -> sqlite3.Cursor:
    _emit_sql_event("before", label, sql_script, operation="SCRIPT")
    started = time.perf_counter()
    try:
        cursor = sqlite3.Connection.executescript(conn, sql_script)
    except sqlite3.Error as exc:
        _emit_sql_event(
            "error",
            label,
            sql_script,
            exc,
            operation="SCRIPT",
            duration_seconds=time.perf_counter() - started,
        )
        raise
    _emit_sql_event(
        "after",
        label,
        sql_script,
        operation="SCRIPT",
        duration_seconds=time.perf_counter() - started,
    )
    return cursor


def sql_cursor_execute(
    cursor: sqlite3.Cursor,
    label: str,
    sql: str,
    parameters: Any = _SQL_NO_PARAMETERS,
) -> sqlite3.Cursor:
    _emit_sql_event("before", label, sql)
    started = time.perf_counter()
    try:
        result = sqlite3.Cursor.execute(cursor, sql, parameters)
    except sqlite3.Error as exc:
        _emit_sql_event(
            "error", label, sql, exc, duration_seconds=time.perf_counter() - started
        )
        raise
    _emit_sql_event("after", label, sql, duration_seconds=time.perf_counter() - started)
    return result


def sql_cursor_executemany(
    cursor: sqlite3.Cursor,
    label: str,
    sql: str,
    seq_of_parameters: Iterable[Any],
) -> sqlite3.Cursor:
    _emit_sql_event("before", label, sql)
    started = time.perf_counter()
    try:
        result = sqlite3.Cursor.executemany(cursor, sql, seq_of_parameters)
    except sqlite3.Error as exc:
        _emit_sql_event(
            "error", label, sql, exc, duration_seconds=time.perf_counter() - started
        )
        raise
    _emit_sql_event("after", label, sql, duration_seconds=time.perf_counter() - started)
    return result


def sql_cursor_executescript(
    cursor: sqlite3.Cursor, label: str, sql_script: str
) -> sqlite3.Cursor:
    _emit_sql_event("before", label, sql_script, operation="SCRIPT")
    started = time.perf_counter()
    try:
        result = sqlite3.Cursor.executescript(cursor, sql_script)
    except sqlite3.Error as exc:
        _emit_sql_event(
            "error",
            label,
            sql_script,
            exc,
            operation="SCRIPT",
            duration_seconds=time.perf_counter() - started,
        )
        raise
    _emit_sql_event(
        "after",
        label,
        sql_script,
        operation="SCRIPT",
        duration_seconds=time.perf_counter() - started,
    )
    return result


def _emit_sql_event(
    phase: str,
    label: str,
    sql: str,
    error: sqlite3.Error | None = None,
    *,
    operation: str | None = None,
    duration_seconds: float | None = None,
) -> None:
    if _sql_event_handler is None:
        return
    _sql_event_handler(
        SqlEvent(
            phase=phase,
            label=label,
            operation=operation or sql_operation(sql),
            sql=sql,
            duration_seconds=duration_seconds,
            error_type=error.__class__.__name__ if error is not None else "",
            error_message=str(error) if error is not None else "",
        )
    )


def sql_operation(sql: str) -> str:
    match = _SQL_OPERATION_RE.match(sql)
    return match.group(1).upper() if match else ""
