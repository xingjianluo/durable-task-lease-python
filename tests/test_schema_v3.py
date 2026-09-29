"""v2（无 lease_token/归档表）到 v3 的平滑升级与数据连续性。"""
from __future__ import annotations

import sqlite3

from tasklease import TaskStatus, TaskStore
from tasklease.schema import SCHEMA_VERSION

from .conftest import claim


def test_v2_database_upgrades_to_v3(tmp_path) -> None:
    db = tmp_path / "v2.db"
    raw = sqlite3.connect(db)
    # 复刻 v2 结构：tasks 无 lease_token/archived_at；task_events 无 lease_token
    raw.execute(
        "CREATE TABLE tasks (id TEXT PRIMARY KEY, idempotency_key TEXT NOT NULL, "
        "payload TEXT NOT NULL DEFAULT '{}', status TEXT NOT NULL, attempt INTEGER "
        "NOT NULL DEFAULT 0, max_attempts INTEGER NOT NULL DEFAULT 1, version INTEGER "
        "NOT NULL DEFAULT 0, lease_owner TEXT, lease_expires_at REAL, available_at "
        "REAL NOT NULL DEFAULT 0.0, last_error TEXT, result TEXT, "
        "created_at REAL NOT NULL, updated_at REAL NOT NULL)"
    )
    raw.execute(
        "CREATE TABLE task_events (id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "task_id TEXT NOT NULL, event TEXT NOT NULL, actor TEXT, from_status TEXT, "
        "to_status TEXT, version INTEGER NOT NULL, reason TEXT, created_at REAL NOT NULL)"
    )
    raw.execute(
        "INSERT INTO tasks (id,idempotency_key,payload,status,attempt,max_attempts,"
        "version,lease_owner,lease_expires_at,available_at,created_at,updated_at) "
        "VALUES ('t1','k1','{}','running',1,3,3,'old-w',100.0,0.0,1.0,2.0)"
    )
    raw.execute(
        "INSERT INTO task_events (task_id,event,actor,from_status,to_status,version,"
        "reason,created_at) VALUES ('t1','acquire','old-w','pending','running',1,'',2.0)"
    )
    raw.commit()
    raw.close()

    st = TaskStore(str(db))
    assert st._conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION  # type: ignore[attr-defined]

    # 旧行可读：lease_token/archived_at 以 NULL 补齐
    t = st.get("t1")
    assert t.status is TaskStatus.RUNNING and t.lease_token is None
    assert t.archived_at is None and t.lease_owner == "old-w"

    # 旧事件可读，lease_token 补 NULL
    evs = st.list_events("t1")
    assert len(evs) == 1 and evs[0].event == "acquire" and evs[0].lease_token is None

    # 归档表已建好可用
    page = st.list_archived_tasks()
    assert page.items == []

    # 旧 RUNNING 租约已过期（expires=100，当前时钟远大于它），可被接管，
    # 接管时发放全新凭证，任务继续走完生命周期
    claimed, tok = claim(st, "new-w", lease_seconds=1000)
    assert claimed.id == "t1" and claimed.attempt == 2 and claimed.lease_token == tok
    done = st.complete("t1", "new-w", tok, {"upgraded": True})
    assert done.status is TaskStatus.SUCCEEDED and done.result == {"upgraded": True}
    st.close()

    # 重开库：结构与终态数据保持稳定（升级幂等）
    st2 = TaskStore(str(db))
    assert st2.get("t1").status is TaskStatus.SUCCEEDED
    assert st2.get_result("t1")["result"] == {"upgraded": True}
    st2.close()
