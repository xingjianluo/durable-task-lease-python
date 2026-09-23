"""端到端兼容：在 v0 旧库文件上直接打开新版 store，旧任务仍可读/可执行。"""

import sqlite3

from task_store.clock import FakeClock
from task_store.store import TaskStore
from task_store.types import CompleteResult, LeaseResult, TaskStatus


def test_open_legacy_db_and_process_old_rows(tmp_path):
    db = str(tmp_path / "legacy.db")
    conn = sqlite3.connect(db)
    conn.execute(
        "CREATE TABLE tasks (id TEXT PRIMARY KEY, idempotency_key TEXT, status TEXT)"
    )
    conn.executemany(
        "INSERT INTO tasks(id, idempotency_key, status) VALUES (?,?,?)",
        [("old-pending", "legacy-p", "PENDING"),
         ("old-running", "legacy-r", "RUNNING")],
    )
    conn.commit()
    conn.close()

    clock = FakeClock(5000.0)  # 远大于旧行的 lease 默认值 0
    store = TaskStore(db, clock, default_lease_seconds=30)

    tasks = {t.id: t for t in store.list_tasks()}
    assert set(tasks) == {"old-pending", "old-running"}
    assert tasks["old-pending"].status is TaskStatus.PENDING
    assert tasks["old-pending"].version == 1  # 旧行获得默认版本

    # 第一次 claim：旧 RUNNING 行被原子回收（available_at=now），但旧 PENDING
    # 行 available_at=0 更早，因此先领取 PENDING，结果为普通 ACQUIRED。
    outcome, lease = store.claim_next("new-consumer")
    assert outcome is LeaseResult.ACQUIRED
    assert lease is not None and lease.task_id == "old-pending"
    assert (
        store.complete(lease.task_id, "new-consumer", lease.fencing_token)
        is CompleteResult.ACCEPTED
    )

    # 第二次领取到上一轮被回收的旧 RUNNING 行，可正常执行
    outcome2, lease2 = store.claim_next("new-consumer")
    assert outcome2 is LeaseResult.ACQUIRED  # 上一事务已回收为普通 PENDING
    assert lease2.task_id == "old-running"
    assert (
        store.complete(lease2.task_id, "new-consumer", lease2.fencing_token)
        is CompleteResult.ACCEPTED
    )

    # 显式验证“过期回收重领”结论：构造一条运行中过期任务，
    # 下一次领取必须返回 EXPIRED_REACQUIRED
    store.enqueue("z-late")
    _, l3 = store.claim_next("c-a")
    clock.advance(100)
    outcome3, lease3 = store.claim_next("c-b")
    assert outcome3 is LeaseResult.EXPIRED_REACQUIRED
    assert lease3.owner == "c-b"

    # 升级后新功能（幂等入队）在旧库上同样工作，且不产生重复
    r1 = store.enqueue("brand-new")
    r2 = store.enqueue("brand-new")
    assert r1.outcome.value == "CREATED" and r2.outcome.value == "DEDUPED_PENDING"
    store.close()
