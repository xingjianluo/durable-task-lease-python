"""SQLite 表结构与平滑升级（迁移）管理。

升级策略：使用 SQLite 内置的 ``PRAGMA user_version`` 记录结构版本，
从 0 开始按顺序增量迁移。旧版本数据库（例如只有一张简单 tasks 表）
升级后其历史行仍可被读取：新增列对旧行填充与新写入一致的默认值。
"""
from __future__ import annotations

import sqlite3

SCHEMA_VERSION = 2

# 新增列的顺序即从“无版本旧库”升级时需要补齐的列。
# (列名, 列定义) —— 默认值保证旧数据可读且语义自洽。
_COLUMN_MIGRATIONS: list[tuple[str, str]] = [
    ("idempotency_key", "TEXT NOT NULL DEFAULT ''"),
    ("payload", "TEXT NOT NULL DEFAULT '{}'"),
    ("attempt", "INTEGER NOT NULL DEFAULT 0"),
    ("max_attempts", "INTEGER NOT NULL DEFAULT 1"),
    ("version", "INTEGER NOT NULL DEFAULT 0"),
    ("lease_owner", "TEXT"),
    ("lease_expires_at", "REAL"),
    ("available_at", "REAL NOT NULL DEFAULT 0.0"),
    ("last_error", "TEXT"),
    ("result", "TEXT"),
    ("created_at", "REAL NOT NULL DEFAULT 0.0"),
    ("updated_at", "REAL NOT NULL DEFAULT 0.0"),
]

_CREATE_TASKS = """
CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL,
    payload TEXT NOT NULL DEFAULT '{}',
    status TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 1,
    version INTEGER NOT NULL DEFAULT 0,
    lease_owner TEXT,
    lease_expires_at REAL,
    available_at REAL NOT NULL DEFAULT 0.0,
    last_error TEXT,
    result TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
)
"""

_CREATE_KEY_INDEX = (
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_tasks_idempotency_key "
    "ON tasks(idempotency_key) WHERE idempotency_key <> ''"
)

_CREATE_EVENTS = """
CREATE TABLE IF NOT EXISTS task_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    event TEXT NOT NULL,
    actor TEXT,
    from_status TEXT,
    to_status TEXT,
    version INTEGER NOT NULL,
    reason TEXT,
    created_at REAL NOT NULL
)
"""


def _column_names(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}


def initialize(conn: sqlite3.Connection) -> int:
    """在已有本地数据库上幂等地建表/升级，旧数据保持可读。

    返回升级后的结构版本。
    """
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    has_tasks = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='tasks'"
    ).fetchone() is not None

    if not has_tasks:
        # 全新数据库：直接建立最新结构。
        conn.execute(_CREATE_TASKS)
        conn.execute(_CREATE_KEY_INDEX)
        conn.execute(_CREATE_EVENTS)
        conn.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
        conn.commit()
        return SCHEMA_VERSION

    # 旧库存在 tasks 表：逐列补齐缺失字段，旧行获得兼容默认值。
    existing = _column_names(conn, "tasks")
    for name, ddl in _COLUMN_MIGRATIONS:
        if name not in existing:
            conn.execute(f"ALTER TABLE tasks ADD COLUMN {name} {ddl}")

    conn.execute(_CREATE_KEY_INDEX)
    conn.execute(_CREATE_EVENTS)
    conn.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
    conn.commit()
    return SCHEMA_VERSION
