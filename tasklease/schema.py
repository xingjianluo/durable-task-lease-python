"""SQLite 表结构与平滑升级（迁移）管理。

升级策略：使用 SQLite 内置的 ``PRAGMA user_version`` 记录结构版本，
从 0 开始按顺序增量迁移。旧版本数据库（例如只有一张简单 tasks 表）
升级后其历史行仍可被读取：新增列对旧行填充与新写入一致的默认值。

结构版本：
- v1（无 user_version 的旧库）：极简 tasks 表。
- v2：幂等键、租约、版本与 task_events 审计表。
- v3：执行凭证 lease_token；结果/历史查询所需的事件指纹；
  终态归档表 tasks_archived / task_events_archived；
  task_keys 幂等结论表（归档后仍能阻止重复入队产生新结论）。
"""
from __future__ import annotations

import sqlite3

SCHEMA_VERSION = 3

# 新增列的顺序即从“无版本旧库”升级时需要补齐的列。
# (表名, 列名, 列定义) —— 默认值保证旧数据可读且语义自洽。
_COLUMN_MIGRATIONS: list[tuple[str, str, str]] = [
    ("tasks", "idempotency_key", "TEXT NOT NULL DEFAULT ''"),
    ("tasks", "payload", "TEXT NOT NULL DEFAULT '{}'"),
    ("tasks", "attempt", "INTEGER NOT NULL DEFAULT 0"),
    ("tasks", "max_attempts", "INTEGER NOT NULL DEFAULT 1"),
    ("tasks", "version", "INTEGER NOT NULL DEFAULT 0"),
    ("tasks", "lease_owner", "TEXT"),
    ("tasks", "lease_expires_at", "REAL"),
    ("tasks", "available_at", "REAL NOT NULL DEFAULT 0.0"),
    ("tasks", "last_error", "TEXT"),
    ("tasks", "result", "TEXT"),
    ("tasks", "created_at", "REAL NOT NULL DEFAULT 0.0"),
    ("tasks", "updated_at", "REAL NOT NULL DEFAULT 0.0"),
    # v3：每次领取都会轮换的执行凭证；旧行 NULL 表示“无凭证的历史租约”。
    ("tasks", "lease_token", "TEXT"),
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
    created_at REAL NOT NULL,
    lease_token TEXT
)
"""

# v3：幂等结论的最终权威表。行一旦写入（含 archived=1）永不删除，
# 因此即使任务从 tasks 移走，重复入队仍能收敛到既有结论。
_CREATE_KEYS = """
CREATE TABLE IF NOT EXISTS task_keys (
    idempotency_key TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    status TEXT NOT NULL,
    archived INTEGER NOT NULL DEFAULT 0
)
"""

_CREATE_KEYS_ARCHIVED_INDEX = (
    "CREATE INDEX IF NOT EXISTS ix_task_keys_archived ON task_keys(archived)"
)

_CREATE_ARCHIVED_TASKS = """
CREATE TABLE IF NOT EXISTS tasks_archived (
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
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    archived_at REAL NOT NULL
)
"""

_CREATE_ARCHIVED_EVENTS = """
CREATE TABLE IF NOT EXISTS task_events_archived (
    id INTEGER PRIMARY KEY,
    task_id TEXT NOT NULL,
    event TEXT NOT NULL,
    actor TEXT,
    from_status TEXT,
    to_status TEXT,
    version INTEGER NOT NULL,
    reason TEXT,
    created_at REAL NOT NULL,
    lease_token TEXT,
    archived_at REAL NOT NULL
)
"""

# 查询辅助索引：归档扫描、历史/任务列表的常见过滤路径。
_CREATE_TASKS_STATUS_INDEX = (
    "CREATE INDEX IF NOT EXISTS ix_tasks_status_updated ON tasks(status, updated_at)"
)
_CREATE_EVENTS_TASK_INDEX = (
    "CREATE INDEX IF NOT EXISTS ix_task_events_task ON task_events(task_id, id)"
)
_CREATE_EVENTS_TIME_INDEX = (
    "CREATE INDEX IF NOT EXISTS ix_task_events_created ON task_events(created_at)"
)
_CREATE_ARCHIVED_UPDATED_INDEX = (
    "CREATE INDEX IF NOT EXISTS ix_tasks_archived_updated "
    "ON tasks_archived(updated_at)"
)
_CREATE_ARCHIVED_EVENTS_TASK_INDEX = (
    "CREATE INDEX IF NOT EXISTS ix_task_events_archived_task "
    "ON task_events_archived(task_id, id)"
)


def _column_names(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}


def _table_names(conn: sqlite3.Connection) -> set[str]:
    return {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }


def _backfill_task_keys(conn: sqlite3.Connection) -> None:
    """从 tasks / tasks_archived 回填幂等结论表。

    幂等键唯一索引保证 tasks 内非空键不重复；INSERT OR IGNORE 让回填
    可以在升级与重开库时反复执行而不产生副作用。
    """
    conn.execute(
        "INSERT OR IGNORE INTO task_keys(idempotency_key, task_id, status, archived) "
        "SELECT idempotency_key, id, status, 0 FROM tasks "
        "WHERE idempotency_key IS NOT NULL AND idempotency_key <> ''"
    )
    tables = _table_names(conn)
    if "tasks_archived" in tables:
        conn.execute(
            "INSERT OR IGNORE INTO task_keys(idempotency_key, task_id, status, archived) "
            "SELECT idempotency_key, id, status, 1 FROM tasks_archived "
            "WHERE idempotency_key IS NOT NULL AND idempotency_key <> ''"
        )


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
        conn.execute(_CREATE_KEYS)
        conn.execute(_CREATE_KEYS_ARCHIVED_INDEX)
        conn.execute(_CREATE_ARCHIVED_TASKS)
        conn.execute(_CREATE_ARCHIVED_EVENTS)
        conn.execute(_CREATE_TASKS_STATUS_INDEX)
        conn.execute(_CREATE_EVENTS_TASK_INDEX)
        conn.execute(_CREATE_EVENTS_TIME_INDEX)
        conn.execute(_CREATE_ARCHIVED_UPDATED_INDEX)
        conn.execute(_CREATE_ARCHIVED_EVENTS_TASK_INDEX)
        conn.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
        conn.commit()
        return SCHEMA_VERSION

    # 旧库存在 tasks 表：逐表逐列补齐缺失字段，旧行获得兼容默认值。
    for table, name, ddl in _COLUMN_MIGRATIONS:
        if table not in _table_names(conn):
            continue
        if name not in _column_names(conn, table):
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}")

    conn.execute(_CREATE_KEY_INDEX)
    conn.execute(_CREATE_EVENTS)
    # 老版本事件表没有指纹列，同样逐列补齐。
    if "lease_token" not in _column_names(conn, "task_events"):
        conn.execute("ALTER TABLE task_events ADD COLUMN lease_token TEXT")

    conn.execute(_CREATE_KEYS)
    conn.execute(_CREATE_KEYS_ARCHIVED_INDEX)
    conn.execute(_CREATE_ARCHIVED_TASKS)
    conn.execute(_CREATE_ARCHIVED_EVENTS)
    conn.execute(_CREATE_TASKS_STATUS_INDEX)
    conn.execute(_CREATE_EVENTS_TASK_INDEX)
    conn.execute(_CREATE_EVENTS_TIME_INDEX)
    conn.execute(_CREATE_ARCHIVED_UPDATED_INDEX)
    conn.execute(_CREATE_ARCHIVED_EVENTS_TASK_INDEX)

    _backfill_task_keys(conn)

    conn.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
    conn.commit()
    return SCHEMA_VERSION
