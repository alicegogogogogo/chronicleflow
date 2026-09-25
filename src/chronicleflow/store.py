from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

# Every tenant-scoped table carries a tenant column. The empty string is the
# legacy namespace used by requests that declare no tenant, so their data and
# behavior are exactly what they were before multi-tenancy existed.
SCHEMA = """
CREATE TABLE IF NOT EXISTS workflows (
  tenant TEXT NOT NULL DEFAULT '',
  id TEXT NOT NULL,
  document TEXT NOT NULL,
  current_version TEXT,
  PRIMARY KEY (tenant, id)
);
CREATE TABLE IF NOT EXISTS workflow_versions (
  tenant TEXT NOT NULL DEFAULT '',
  workflow_id TEXT NOT NULL,
  version TEXT NOT NULL,
  position INTEGER NOT NULL,
  document TEXT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY (tenant, workflow_id, version),
  FOREIGN KEY (tenant, workflow_id) REFERENCES workflows(tenant, id)
);
CREATE TABLE IF NOT EXISTS executions (
  tenant TEXT NOT NULL DEFAULT '',
  id TEXT NOT NULL,
  workflow_id TEXT NOT NULL,
  state TEXT NOT NULL,
  workflow_version TEXT,
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
  version TEXT NOT NULL DEFAULT '',
  position INTEGER NOT NULL,
  document TEXT NOT NULL,
  PRIMARY KEY (tenant, owner_type, owner_id, version, position)
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
CREATE TABLE IF NOT EXISTS quotas (
  tenant TEXT PRIMARY KEY,
  workflows INTEGER NOT NULL,
  executions INTEGER NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS usage_records (
  tenant TEXT NOT NULL,
  sequence INTEGER NOT NULL,
  usage_type TEXT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY (tenant, sequence)
);
"""

# Legacy (pre-tenancy) column layouts, used only when migrating an old file.
LEGACY_COLUMNS = {
    "workflows": "id, document, NULL",
    "executions": "id, workflow_id, state, NULL",
    "events": "execution_id, sequence, type, payload, occurred_at",
    "idempotency": "key, operation, response",
    "checkpoints": "execution_id, sequence, event_sequence, document, created_at",
    "leases": "execution_id, worker_id, lease_seconds, expires_at, heartbeat_at",
    "subscriptions": "owner_type, owner_id, '', position, document",
    "deliveries": "execution_id, sequence, document",
    "schedules": (
        "workflow_id, document, paused, anchor_at, cursor, last_triggered_at, last_execution_id"
    ),
    "schedule_triggers": "workflow_id, period_key, execution_id, triggered_at",
}

# Columns introduced after a table first existed; added additively so databases
# created on the baseline schema keep working without a table rebuild.
ADDED_COLUMNS = {
    "workflows": ("current_version", "TEXT"),
    "executions": ("workflow_version", "TEXT"),
    "subscriptions": ("version", "TEXT NOT NULL DEFAULT ''"),
}


class Store:
    def __init__(self, path: str):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
        # Serializes transactions across request threads and the scheduler thread.
        self._lock = threading.RLock()
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.connection.execute("PRAGMA journal_mode = WAL")
        self._migrate_legacy_schema()
        self._migrate_added_columns()
        self.connection.executescript(SCHEMA)
        self._backfill_versions()
        self._normalize_delivery_attempts()

    def _normalize_delivery_attempts(self) -> None:
        """Align persisted delivery attempts with the documented field set.

        Older builds recorded an ``attempt`` number inside each entry of the
        ``attempts`` list; the documented record carries only the try's
        ``status_code`` or ``error``. Rewrite stored entries once so the
        delivery history is consistent across upgrades.
        """
        rows = self.connection.execute("SELECT rowid, document FROM deliveries").fetchall()
        for row in rows:
            try:
                document = json.loads(row["document"])
            except ValueError:
                continue
            attempts = document.get("attempts")
            if not isinstance(attempts, list):
                continue
            changed = False
            for attempt in attempts:
                if isinstance(attempt, dict) and attempt.pop("attempt", None) is not None:
                    changed = True
            if changed:
                self.connection.execute(
                    "UPDATE deliveries SET document = ? WHERE rowid = ?",
                    (
                        json.dumps(document, ensure_ascii=False, separators=(",", ":"), sort_keys=True),
                        row["rowid"],
                    ),
                )

    def _migrate_legacy_schema(self) -> None:
        """Rebuild tables created before tenancy, moving every row to the default namespace."""
        table_rows = self.connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        columns = {}
        for table_row in table_rows:
            name = table_row["name"]
            columns[name] = [info["name"] for info in self.connection.execute(f'PRAGMA table_info("{name}")')]
        legacy = [name for name in LEGACY_COLUMNS if name in columns and "tenant" not in columns[name]]
        if not legacy:
            return
        self.connection.execute("PRAGMA foreign_keys = OFF")
        self.connection.execute("BEGIN IMMEDIATE")
        table_statements = {}
        for part in SCHEMA.split(";"):
            statement = part.strip()
            if statement:
                table_statements[statement.split("(", 1)[0].strip().split()[-1]] = statement
        try:
            for name in legacy:
                old_columns = LEGACY_COLUMNS[name]
                self.connection.execute(f'ALTER TABLE "{name}" RENAME TO "{name}_legacy"')
                self.connection.execute(table_statements[name])
                self.connection.execute(
                    f"INSERT INTO {name} SELECT '', {old_columns} FROM {name}_legacy"
                )
                self.connection.execute(f"DROP TABLE {name}_legacy")
        except Exception:
            self.connection.execute("ROLLBACK")
            raise
        else:
            self.connection.execute("COMMIT")
        finally:
            self.connection.execute("PRAGMA foreign_keys = ON")

    def _migrate_added_columns(self) -> None:
        """Add columns introduced after a table first existed to an existing database."""
        for table, (column, declaration) in ADDED_COLUMNS.items():
            existing = [info["name"] for info in self.connection.execute(f'PRAGMA table_info("{table}")')]
            if not existing or column in existing:
                continue
            self.connection.execute(f'ALTER TABLE "{table}" ADD COLUMN {column} {declaration}')

    def _backfill_versions(self) -> None:
        """Give workflows created before versioning an immutable unversioned revision."""
        self.connection.execute(
            "INSERT INTO workflow_versions(tenant, workflow_id, version, position, document, created_at) "
            "SELECT w.tenant, w.id, '', 0, w.document, strftime('%Y-%m-%dT%H:%M:%fZ', 'now') "
            "FROM workflows w WHERE NOT EXISTS ( "
            "SELECT 1 FROM workflow_versions v WHERE v.tenant = w.tenant AND v.workflow_id = w.id)"
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
