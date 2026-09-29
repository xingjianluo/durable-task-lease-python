"""SQLite 表结构与平滑升级（迁移）管理。

升级策略：使用 SQLite 内置的 ``PRAGMA user_version`` 记录结构版本，
从 0 开始按顺序增量迁移。旧版本数据库（例如只有一张简单 tasks 表）
升级后其历史行仍可被读取：新增列对旧行填充与新写入一致的默认值。
"""
from __future__ import annotations

import sqlite3

SCHEMA_VERSION = 3

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
    # v3：每次领取发放的一次性执行凭证；归档时间。
    ("lease_token", "TEXT"),
    ("archived_at", "REAL"),
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
    lease_token TEXT,
    available_at REAL NOT NULL DEFAULT 0.0,
    last_error TEXT,
    result TEXT,
    archived_at REAL,
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
    lease_token TEXT,
    from_status TEXT,
    to_status TEXT,
    version INTEGER NOT NULL,
    reason TEXT,
    created_at REAL NOT NULL
)
"""

# 归档表：列与主表一致，另加 archived_at；终结很久的任务搬到这里，
# 主表保持精简，新任务的领取/提交不受归档过程影响。
_CREATE_TASKS_ARCHIVE = """
CREATE TABLE IF NOT EXISTS tasks_archive (
    id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL,
    payload TEXT NOT NULL DEFAULT '{}',
    status TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 1,
    version INTEGER NOT NULL DEFAULT 0,
    lease_owner TEXT,
    lease_expires_at REAL,
    lease_token TEXT,
    available_at REAL NOT NULL DEFAULT 0.0,
    last_error TEXT,
    result TEXT,
    archived_at REAL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
)
"""

_CREATE_ARCHIVE_KEY_INDEX = (
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_tasks_archive_idempotency_key "
    "ON tasks_archive(idempotency_key) WHERE idempotency_key <> ''"
)

_CREATE_EVENTS_ARCHIVE = """
CREATE TABLE IF NOT EXISTS task_events_archive (
    id INTEGER PRIMARY KEY,
    task_id TEXT NOT NULL,
    event TEXT NOT NULL,
    actor TEXT,
    lease_token TEXT,
    from_status TEXT,
    to_status TEXT,
    version INTEGER NOT NULL,
    reason TEXT,
    created_at REAL NOT NULL
)
"""
_CREATE_EVENTS_ARCHIVE_INDEX = (
    "CREATE INDEX IF NOT EXISTS ix_task_events_archive_task "
    "ON task_events_archive(task_id, id)"
)
_CREATE_EVENTS_INDEX = (
    "CREATE INDEX IF NOT EXISTS ix_task_events_task ON task_events(task_id, id)"
)
_CREATE_TASKS_STATUS_INDEX = (
    "CREATE INDEX IF NOT EXISTS ix_tasks_status_updated "
    "ON tasks(status, updated_at)"
)


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
        conn.execute(_CREATE_TASKS_STATUS_INDEX)
        conn.execute(_CREATE_EVENTS)
        conn.execute(_CREATE_EVENTS_INDEX)
        conn.execute(_CREATE_TASKS_ARCHIVE)
        conn.execute(_CREATE_ARCHIVE_KEY_INDEX)
        conn.execute(_CREATE_EVENTS_ARCHIVE)
        conn.execute(_CREATE_EVENTS_ARCHIVE_INDEX)
        conn.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
        conn.commit()
        return SCHEMA_VERSION

    # 旧库存在 tasks 表：逐列补齐缺失字段，旧行获得兼容默认值。
    existing = _column_names(conn, "tasks")
    for name, ddl in _COLUMN_MIGRATIONS:
        if name not in existing:
            conn.execute(f"ALTER TABLE tasks ADD COLUMN {name} {ddl}")

    conn.execute(_CREATE_KEY_INDEX)
    conn.execute(_CREATE_TASKS_STATUS_INDEX)

    # 事件表：旧库可能没有，也可能是缺 lease_token 列的旧版本。
    has_events = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='task_events'"
    ).fetchone() is not None
    conn.execute(_CREATE_EVENTS)
    if "lease_token" not in _column_names(conn, "task_events"):
        conn.execute("ALTER TABLE task_events ADD COLUMN lease_token TEXT")
    conn.execute(_CREATE_EVENTS_INDEX)

    conn.execute(_CREATE_TASKS_ARCHIVE)
    conn.execute(_CREATE_ARCHIVE_KEY_INDEX)
    conn.execute(_CREATE_EVENTS_ARCHIVE)
    conn.execute(_CREATE_EVENTS_ARCHIVE_INDEX)
    conn.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
    conn.commit()
    return SCHEMA_VERSION
