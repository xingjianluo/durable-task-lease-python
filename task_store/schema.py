"""SQLite schema 与本地数据平滑升级（version 0 -> version 1）。

升级策略
--------
- 旧版本地数据（v0：无 schema_meta，或已存在结构更简的 tasks 表）仍可正常读取；
- 采用“加列不删列、加表不破坏旧表”的方式做幂等迁移，保留所有旧数据行；
- 版本号同时记录在 schema_meta 表与 PRAGMA user_version 中，任一可读即可判定。
"""

from __future__ import annotations

import sqlite3

SCHEMA_VERSION = 1

# 完整 v1 建表语句（IF NOT EXISTS，可在旧库 / 空库上执行）。
_CREATE_TASKS = """
CREATE TABLE IF NOT EXISTS tasks (
    id                  TEXT PRIMARY KEY,
    idempotency_key     TEXT NOT NULL,
    payload             TEXT NOT NULL DEFAULT '',
    status              TEXT NOT NULL DEFAULT 'PENDING',
    available_at        REAL NOT NULL DEFAULT 0,
    lease_owner         TEXT,
    lease_expires_at    REAL,
    fencing_token       INTEGER NOT NULL DEFAULT 0,
    attempt_count       INTEGER NOT NULL DEFAULT 0,
    max_attempts        INTEGER NOT NULL DEFAULT 3,
    backoff_base_seconds REAL NOT NULL DEFAULT 1.0,
    backoff_factor      REAL NOT NULL DEFAULT 2.0,
    version             INTEGER NOT NULL DEFAULT 1,
    result_payload      TEXT,
    error_message       TEXT,
    created_at          REAL NOT NULL DEFAULT 0,
    updated_at          REAL NOT NULL DEFAULT 0
)
"""

_CREATE_IDEMPOTENCY_INDEX = (
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_tasks_idempotency_key "
    "ON tasks(idempotency_key)"
)

_CREATE_META = """
CREATE TABLE IF NOT EXISTS schema_meta (
    version INTEGER PRIMARY KEY,
    applied_at REAL NOT NULL
)
"""

# v0 -> v1：为可能存在的旧 tasks 表补齐缺失列。SQLite 不支持
# ADD COLUMN IF NOT EXISTS，因此先查 PRAGMA table_info 再按需 ADD；
# 所有新增列都带默认值，旧行自动得到默认值，旧数据升级后可正常读取。
_V1_ADDED_COLUMNS: tuple[tuple[str, str], ...] = (
    ("payload", "TEXT NOT NULL DEFAULT ''"),
    ("available_at", "REAL NOT NULL DEFAULT 0"),
    ("lease_owner", "TEXT"),
    ("lease_expires_at", "REAL"),
    ("fencing_token", "INTEGER NOT NULL DEFAULT 0"),
    ("attempt_count", "INTEGER NOT NULL DEFAULT 0"),
    ("max_attempts", "INTEGER NOT NULL DEFAULT 3"),
    ("backoff_base_seconds", "REAL NOT NULL DEFAULT 1.0"),
    ("backoff_factor", "REAL NOT NULL DEFAULT 2.0"),
    ("version", "INTEGER NOT NULL DEFAULT 1"),
    ("result_payload", "TEXT"),
    ("error_message", "TEXT"),
    ("created_at", "REAL NOT NULL DEFAULT 0"),
    ("updated_at", "REAL NOT NULL DEFAULT 0"),
)


def get_user_version(conn: sqlite3.Connection) -> int:
    """读取当前数据库 schema 版本。

    优先读 PRAGMA user_version（权威）；为兼容只写了 meta 表的库，再查
    schema_meta。都不存在时返回 0（旧库/空库）。
    """
    cur = conn.execute("PRAGMA user_version")
    uv = int(cur.fetchone()[0])
    if uv:
        return uv
    try:
        cur = conn.execute("SELECT MAX(version) FROM schema_meta")
        row = cur.fetchone()
        if row and row[0] is not None:
            return int(row[0])
    except sqlite3.OperationalError:
        pass
    return 0


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}


def migrate(conn: sqlite3.Connection, *, now: float = 0.0) -> int:
    """把数据库升级到当前 SCHEMA_VERSION，返回升级后的版本号。

    幂等：已是最新版本时直接返回，不做任何变更。
    """
    current = get_user_version(conn)
    if current >= SCHEMA_VERSION:
        return current

    # v0 -> v1：建表 / 补列 / 建唯一索引，全部 IF NOT EXISTS / 按需 ADD。
    conn.execute(_CREATE_TASKS)
    existing = _table_columns(conn, "tasks")
    added = [name for name, decl in _V1_ADDED_COLUMNS if name not in existing]
    for name in added:
        decl = dict(_V1_ADDED_COLUMNS)[name]
        conn.execute(f"ALTER TABLE tasks ADD COLUMN {name} {decl}")
    conn.execute(_CREATE_IDEMPOTENCY_INDEX)

    # 关键兼容处理：SQLite 对“可空列”的 ADD COLUMN 不会给旧行填默认值
    # （旧行该列为 NULL）。因此对旧数据做一次性回填，使其语义完整：
    # 旧版没有租约概念，遗留的 RUNNING 行视为“租约已过期、无主”，
    # 升级后可被新消费者正常回收与领取。
    if added:
        conn.execute(
            "UPDATE tasks SET lease_expires_at = 0 "
            "WHERE status = 'RUNNING' AND lease_expires_at IS NULL"
        )
        conn.execute(
            "UPDATE tasks SET available_at = 0 WHERE available_at IS NULL"
        )

    conn.execute(_CREATE_META)
    conn.execute(
        "INSERT OR REPLACE INTO schema_meta(version, applied_at) VALUES (?, ?)",
        (SCHEMA_VERSION, now),
    )
    conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
    conn.commit()
    return SCHEMA_VERSION
