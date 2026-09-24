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
        self.connection = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.connection.execute("PRAGMA journal_mode = WAL")
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
            """
        )

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

