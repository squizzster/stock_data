from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

import pytest

from stock_universe.storage import (
    DEFAULT_SQLITE_BUSY_TIMEOUT_MS,
    connect_sqlite,
    connect_readonly_sqlite,
    install_slow_sql_logger,
    set_sql_event_handler,
    sql_select_all,
    sql_select_one,
)


def test_managed_sqlite_connections_emit_sql_events(tmp_path: Path) -> None:
    events = []
    set_sql_event_handler(events.append)
    try:
        with connect_sqlite(tmp_path / "stock_universe.sqlite") as conn:
            conn.execute("CREATE TABLE demo(id INTEGER PRIMARY KEY, value TEXT)")
            conn.execute("INSERT INTO demo(value) VALUES (?)", ("x",))
            rows = sql_select_all(conn, "demo.select_all", "SELECT value FROM demo")
    finally:
        set_sql_event_handler(None)

    assert [row["value"] for row in rows] == ["x"]
    assert any(
        event.phase == "after" and event.label == "connection.execute"
        for event in events
    )
    assert any(
        event.phase == "after"
        and event.label == "demo.select_all"
        and event.operation == "SELECT"
        and event.duration_seconds is not None
        for event in events
    )


def test_slow_sql_logger_can_be_installed_per_process(tmp_path: Path) -> None:
    records: list[logging.LogRecord] = []

    class ListHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    logger = logging.getLogger("tests.stock_universe.slow_sql")
    logger.handlers = [ListHandler()]
    logger.propagate = False
    logger.setLevel(logging.WARNING)

    install_slow_sql_logger(threshold_seconds=0.0, logger=logger)
    try:
        with connect_sqlite(tmp_path / "stock_universe.sqlite") as conn:
            row = sql_select_one(conn, "demo.select_one", "SELECT 1 AS value")
    finally:
        set_sql_event_handler(None)
        logger.handlers = []

    assert row is not None
    assert row["value"] == 1
    assert any(
        "slow sqlite SELECT label=demo.select_one" in record.getMessage()
        for record in records
    )


def test_managed_sqlite_connections_set_busy_timeout(tmp_path: Path) -> None:
    db = tmp_path / "stock_universe.sqlite"
    with connect_sqlite(db) as conn:
        conn.execute("CREATE TABLE demo(id INTEGER PRIMARY KEY)")
        write_timeout = conn.execute("PRAGMA busy_timeout").fetchone()[0]

    with connect_readonly_sqlite(db) as conn:
        read_timeout = conn.execute("PRAGMA busy_timeout").fetchone()[0]

    assert write_timeout == DEFAULT_SQLITE_BUSY_TIMEOUT_MS
    assert read_timeout == DEFAULT_SQLITE_BUSY_TIMEOUT_MS


def test_readonly_connections_are_query_only_and_read_during_write_transaction(
    tmp_path: Path,
) -> None:
    db = tmp_path / "stock_universe.sqlite"
    with connect_sqlite(db) as conn:
        conn.execute("CREATE TABLE demo(id INTEGER PRIMARY KEY, value TEXT)")
        conn.execute("INSERT INTO demo(value) VALUES ('committed')")
        conn.commit()

    writer = connect_sqlite(db)
    try:
        writer.execute("BEGIN IMMEDIATE")
        writer.execute("INSERT INTO demo(value) VALUES ('uncommitted')")

        with connect_readonly_sqlite(db) as first_reader:
            assert first_reader.execute("PRAGMA query_only").fetchone()[0] == 1
            assert [
                row["value"]
                for row in first_reader.execute(
                    "SELECT value FROM demo ORDER BY id"
                ).fetchall()
            ] == ["committed"]
            with pytest.raises(sqlite3.OperationalError, match="readonly|read-only"):
                first_reader.execute("INSERT INTO demo(value) VALUES ('blocked')")

        with connect_readonly_sqlite(db) as second_reader:
            assert [
                row["value"]
                for row in second_reader.execute(
                    "SELECT value FROM demo ORDER BY id"
                ).fetchall()
            ] == ["committed"]
    finally:
        writer.rollback()
        writer.close()
