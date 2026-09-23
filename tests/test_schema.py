"""schema 升级测试：v0 旧库平滑升级、幂等迁移、旧数据可读。"""

import sqlite3

from task_store.schema import SCHEMA_VERSION, get_user_version, migrate


def test_migrate_empty_db(tmp_path):
    db = tmp_path / "t.db"
    conn = sqlite3.connect(db)
    assert get_user_version(conn) == 0
    assert migrate(conn, now=1.0) == SCHEMA_VERSION
    # 幂等：再跑一次不报错，版本不变
    assert migrate(conn, now=2.0) == SCHEMA_VERSION
    names = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"tasks", "schema_meta"} <= names
    conn.close()


def test_migrate_from_legacy_v0_data(tmp_path):
    """模拟旧版本地数据：简版 tasks 表 + 旧行，升级后旧行可读且获得默认值。"""
    db = tmp_path / "legacy.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE tasks (id TEXT PRIMARY KEY, idempotency_key TEXT, status TEXT)")
    conn.execute("INSERT INTO tasks(id, idempotency_key, status) VALUES ('old-1', 'key-1', 'PENDING')")
    conn.execute("INSERT INTO tasks(id, idempotency_key, status) VALUES ('old-2', 'key-2', 'RUNNING')")
    conn.commit()

    assert migrate(conn, now=10.0) == SCHEMA_VERSION
    assert get_user_version(conn) == SCHEMA_VERSION

    rows = list(conn.execute(
        "SELECT id, status, version, fencing_token, max_attempts, available_at, "
        "lease_expires_at FROM tasks ORDER BY id"
    ))
    # 非空带默认值的列自动回填默认值；可空列 lease_expires_at 经迁移显式回填：
    # 遗留 RUNNING 行视为租约已过期（0），保证升级后可被回收重领。
    assert rows == [
        ("old-1", "PENDING", 1, 0, 3, 0.0, None),
        ("old-2", "RUNNING", 1, 0, 3, 0.0, 0.0),
    ]
    # 唯一索引生效：重复幂等键无法插入
    import sqlite3 as _s
    try:
        conn.execute(
            "INSERT INTO tasks(id, idempotency_key, version, created_at, updated_at) "
            "VALUES ('x', 'key-1', 1, 0, 0)"
        )
        raise AssertionError("expected IntegrityError")
    except _s.IntegrityError:
        conn.rollback()
    conn.close()
