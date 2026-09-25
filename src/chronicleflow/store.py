from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator


class Store:
    def __init__(self, path: str):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
        # Serializes transactions across request threads and the scheduler thread.
        self._lock = threading.RLock()
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.connection.execute("PRAGMA journal_mode = WAL")
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS workflows (
              tenant TEXT NOT NULL DEFAULT '',
              id TEXT NOT NULL,
              document TEXT NOT NULL,
              PRIMARY KEY (tenant, id)
            );
            CREATE TABLE IF NOT EXISTS executions (
              tenant TEXT NOT NULL DEFAULT '',
              id TEXT NOT NULL,
              workflow_id TEXT NOT NULL,
              state TEXT NOT NULL,
              PRIMARY KEY (tenant, id),
              FOREIGN KEY (tenant, workflow_id) REFERENCES workflows(tenant, id)
            );
            CREATE TABLE IF NOT EXISTS events (
              tenant TEXT NOT NULL DEFAULT '',
              execution_id TEXT NOT NULL,
              sequence INTEGER NOT NULL,
              type TEXT NOT NULL,
              payload TEXT NOT NULL,
              occurred_at TEXT NOT NULL,
              PRIMARY KEY (tenant, execution_id, sequence),
              FOREIGN KEY (tenant, execution_id) REFERENCES executions(tenant, id)
            );
            CREATE TABLE IF NOT EXISTS idempotency (
              tenant TEXT NOT NULL DEFAULT '',
              key TEXT NOT NULL,
              operation TEXT NOT NULL,
              response TEXT NOT NULL,
              PRIMARY KEY (tenant, key)
            );
            CREATE TABLE IF NOT EXISTS checkpoints (
              tenant TEXT NOT NULL DEFAULT '',
              execution_id TEXT NOT NULL,
              sequence INTEGER NOT NULL,
              event_sequence INTEGER NOT NULL,
              document TEXT NOT NULL,
              created_at TEXT NOT NULL,
              PRIMARY KEY (tenant, execution_id, sequence),
              FOREIGN KEY (tenant, execution_id) REFERENCES executions(tenant, id)
            );
            CREATE TABLE IF NOT EXISTS leases (
              tenant TEXT NOT NULL DEFAULT '',
              execution_id TEXT NOT NULL,
              worker_id TEXT NOT NULL,
              lease_seconds REAL NOT NULL,
              expires_at REAL NOT NULL,
              heartbeat_at REAL NOT NULL,
              PRIMARY KEY (tenant, execution_id),
              FOREIGN KEY (tenant, execution_id) REFERENCES executions(tenant, id)
            );
            CREATE TABLE IF NOT EXISTS subscriptions (
              tenant TEXT NOT NULL DEFAULT '',
              owner_type TEXT NOT NULL,
              owner_id TEXT NOT NULL,
              position INTEGER NOT NULL,
              document TEXT NOT NULL,
              PRIMARY KEY (tenant, owner_type, owner_id, position)
            );
            CREATE TABLE IF NOT EXISTS deliveries (
              tenant TEXT NOT NULL DEFAULT '',
              execution_id TEXT NOT NULL,
              sequence INTEGER NOT NULL,
              document TEXT NOT NULL,
              PRIMARY KEY (tenant, execution_id, sequence),
              FOREIGN KEY (tenant, execution_id) REFERENCES executions(tenant, id)
            );
            CREATE TABLE IF NOT EXISTS schedules (
              tenant TEXT NOT NULL DEFAULT '',
              workflow_id TEXT NOT NULL,
              document TEXT NOT NULL,
              paused INTEGER NOT NULL DEFAULT 0,
              anchor_at REAL NOT NULL,
              cursor TEXT NOT NULL DEFAULT '',
              last_triggered_at TEXT,
              last_execution_id TEXT,
              PRIMARY KEY (tenant, workflow_id),
              FOREIGN KEY (tenant, workflow_id) REFERENCES workflows(tenant, id)
            );
            CREATE TABLE IF NOT EXISTS schedule_triggers (
              tenant TEXT NOT NULL DEFAULT '',
              workflow_id TEXT NOT NULL,
              period_key TEXT NOT NULL,
              execution_id TEXT NOT NULL,
              triggered_at TEXT NOT NULL,
              PRIMARY KEY (tenant, workflow_id, period_key),
              FOREIGN KEY (tenant, workflow_id) REFERENCES workflows(tenant, id)
            );
            CREATE TABLE IF NOT EXISTS tenant_quotas (
              tenant TEXT PRIMARY KEY,
              max_workflows INTEGER NOT NULL,
              max_executions INTEGER NOT NULL
            );
            """
        )

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
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

