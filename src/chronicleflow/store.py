from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator


class Store:
    def __init__(self, path: str):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.connection = self._connect()
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS workflows (
              id TEXT PRIMARY KEY,
              document TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS executions (
              id TEXT PRIMARY KEY,
              workflow_id TEXT NOT NULL REFERENCES workflows(id),
              state TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS events (
              execution_id TEXT NOT NULL REFERENCES executions(id),
              sequence INTEGER NOT NULL,
              type TEXT NOT NULL,
              payload TEXT NOT NULL,
              occurred_at TEXT NOT NULL,
              PRIMARY KEY (execution_id, sequence)
            );
            CREATE TABLE IF NOT EXISTS idempotency (
              key TEXT PRIMARY KEY,
              operation TEXT NOT NULL,
              response TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS checkpoints (
              execution_id TEXT NOT NULL REFERENCES executions(id),
              sequence INTEGER NOT NULL,
              event_sequence INTEGER NOT NULL,
              document TEXT NOT NULL,
              created_at TEXT NOT NULL,
              PRIMARY KEY (execution_id, sequence)
            );
            CREATE TABLE IF NOT EXISTS leases (
              execution_id TEXT PRIMARY KEY REFERENCES executions(id),
              worker_id TEXT NOT NULL,
              lease_seconds REAL NOT NULL,
              expires_at REAL NOT NULL,
              heartbeat_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS subscriptions (
              owner_type TEXT NOT NULL,
              owner_id TEXT NOT NULL,
              position INTEGER NOT NULL,
              document TEXT NOT NULL,
              PRIMARY KEY (owner_type, owner_id, position)
            );
            CREATE TABLE IF NOT EXISTS deliveries (
              execution_id TEXT NOT NULL REFERENCES executions(id),
              sequence INTEGER NOT NULL,
              document TEXT NOT NULL,
              PRIMARY KEY (execution_id, sequence)
            );
            CREATE TABLE IF NOT EXISTS schedules (
              workflow_id TEXT PRIMARY KEY REFERENCES workflows(id),
              document TEXT NOT NULL,
              paused INTEGER NOT NULL DEFAULT 0,
              activated_at REAL NOT NULL,
              last_fired_at REAL,
              last_execution_id TEXT
            );
            CREATE TABLE IF NOT EXISTS schedule_periods (
              workflow_id TEXT NOT NULL,
              period_start REAL NOT NULL,
              execution_id TEXT NOT NULL,
              created_at TEXT NOT NULL,
              PRIMARY KEY (workflow_id, period_start)
            );
            CREATE TABLE IF NOT EXISTS schedule_events (
              sequence INTEGER PRIMARY KEY AUTOINCREMENT,
              workflow_id TEXT NOT NULL,
              execution_id TEXT NOT NULL,
              period_start REAL NOT NULL,
              occurred_at TEXT NOT NULL
            );
            """
        )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, isolation_level=None, check_same_thread=False)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        # The request connection and the scheduler connection can contend for
        # the write lock; wait briefly rather than failing immediately.
        connection.execute("PRAGMA busy_timeout = 5000")
        return connection

    def aux_connection(self) -> sqlite3.Connection:
        """A separate connection for the background scheduler thread."""
        return self._connect()

    def close(self) -> None:
        self.connection.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            yield self.connection
        except Exception:
            self.connection.execute("ROLLBACK")
            raise
        else:
            self.connection.execute("COMMIT")

    @staticmethod
    def encode(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)

    @staticmethod
    def decode(value: str) -> Any:
        return json.loads(value)

    @staticmethod
    def now() -> str:
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

