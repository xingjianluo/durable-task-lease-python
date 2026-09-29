"""幂等入队与非法状态流转测试。"""
from __future__ import annotations

import threading

import pytest

from tasklease import (
    EnqueueOutcome,
    RejectReason,
    TaskRejected,
    TaskStatus,
    TaskStore,
)
from tasklease.models import Task

from ._logutil import log_events, log_task
from .conftest import claim


def test_duplicate_enqueue_converges_to_same_task(store: TaskStore) -> None:
    t1, o1 = store.enqueue("order-1", {"v": 1}, max_attempts=3)
    t2, o2 = store.enqueue("order-1", {"v": 1})
    t3, o3 = store.enqueue("order-1", {"different": True})  # 负载不同也收敛
    log_task("首次入队", t1)
    log_task("重复入队命中", t2)

    assert o1 is EnqueueOutcome.CREATED
    assert o2 is EnqueueOutcome.EXISTING
    assert o3 is EnqueueOutcome.EXISTING
    assert {t1.id, t2.id, t3.id} == {t1.id}
    assert store.get_by_idempotency_key("order-1").id == t1.id
    log_events(store._conn, t1.id)  # type: ignore[attr-defined]


def test_no_duplicate_rows_under_concurrent_enqueue(tmp_path) -> None:
    db = tmp_path / "conc.db"
    stores = [TaskStore(str(db)) for _ in range(4)]
    ids: list[str] = []
    barrier = threading.Barrier(4)

    def worker(st: TaskStore) -> None:
        barrier.wait()
        task, _ = st.enqueue("hot-key", {"x": 1})
        ids.append(task.id)

    threads = [threading.Thread(target=worker, args=(s,)) for s in stores]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(set(ids)) == 1, f"并发入队产生多条记录: {ids}"
    stores[0].close()


def test_enqueue_rejected_while_executing(store: TaskStore, clock) -> None:
    task, _ = store.enqueue("running-key")
    log_task("入队后", task)
    claimed, tok = claim(store, "worker-A", lease_seconds=10)
    log_task("被A领取", claimed)

    with pytest.raises(TaskRejected) as exc:
        store.enqueue("running-key")
    assert exc.value.reason is RejectReason.TASK_EXECUTING
    log_events(store._conn, task.id)  # type: ignore[attr-defined]

    # 租约过期后再次提交：不拒绝，收敛到同一任务（等待被重新领取）
    clock.advance(11)
    same, outcome = store.enqueue("running-key")
    assert same.id == task.id
    assert outcome is EnqueueOutcome.EXISTING
    log_task("租约过期后重复提交", same)


def test_enqueue_rejected_for_terminal_tasks(store: TaskStore) -> None:
    for terminal_action in ("succeed", "fail"):
        key = f"terminal-{terminal_action}"
        task, _ = store.enqueue(key, max_attempts=1)
        claimed, tok = claim(store, "w")
        if terminal_action == "succeed":
            store.complete(claimed.id, "w", tok)
        else:
            store.fail(claimed.id, "w", tok, "boom")
        log_task(f"终态({terminal_action})", store.get(task.id))

        with pytest.raises(TaskRejected) as exc:
            store.enqueue(key)
        assert exc.value.reason is RejectReason.TASK_TERMINAL
        assert store.get(task.id).is_terminal()


def test_enqueue_rejected_for_corrupt_legacy_running(tmp_path) -> None:
    """旧版本遗留的缺字段 RUNNING：重复入队被 lease_corrupt 拒绝并提示恢复。"""
    import sqlite3

    from tasklease import RunningLeaseState

    db = tmp_path / "corrupt.db"
    raw = sqlite3.connect(db)
    raw.execute(
        "CREATE TABLE tasks (id TEXT PRIMARY KEY, status TEXT NOT NULL, "
        "idempotency_key TEXT NOT NULL DEFAULT '')"
    )
    raw.execute(
        "INSERT INTO tasks (id,status,idempotency_key) VALUES ('x','running','corrupt-k')"
    )
    raw.commit(); raw.close()

    st = TaskStore(str(db))
    assert st.classify_running("x") is RunningLeaseState.CORRUPT
    with pytest.raises(TaskRejected) as exc:
        st.enqueue("corrupt-k")
    assert exc.value.reason is RejectReason.LEASE_CORRUPT
    st.close()


def test_invalid_transitions_have_distinct_reasons(store: TaskStore) -> None:
    # 未领取（PENDING）直接完成 -> 非法流转，原因可区分
    task, _ = store.enqueue("k-pending")
    with pytest.raises(TaskRejected) as exc:
        store.complete(task.id, "nobody", "any-token")
    assert exc.value.reason is RejectReason.NOT_PENDING_OR_UNCLAIMED

    claimed, tok = claim(store, "owner")
    # 非持有者
    with pytest.raises(TaskRejected) as exc:
        store.complete(task.id, "intruder", tok)
    assert exc.value.reason is RejectReason.LEASE_HELD_BY_OTHER
    # 持有者但凭证错误
    with pytest.raises(TaskRejected) as exc:
        store.complete(task.id, "owner", "forged-token")
    assert exc.value.reason is RejectReason.INVALID_TOKEN

    # 不存在的任务
    with pytest.raises(TaskRejected) as exc:
        store.complete("missing", "owner", "t")
    assert exc.value.reason is RejectReason.TASK_NOT_FOUND


def test_invalid_max_attempts(store: TaskStore) -> None:
    with pytest.raises(ValueError):
        store.enqueue("bad", max_attempts=0)
