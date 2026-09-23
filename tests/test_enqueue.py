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
    claimed = store.acquire("worker-A", lease_seconds=10)
    assert claimed is not None
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
        claimed = store.acquire("w")
        if terminal_action == "succeed":
            store.complete(claimed.id, "w")
        else:
            store.fail(claimed.id, "w", "boom")
        log_task(f"终态({terminal_action})", store.get(task.id))

        with pytest.raises(TaskRejected) as exc:
            store.enqueue(key)
        assert exc.value.reason is RejectReason.TASK_TERMINAL
        assert store.get(task.id).is_terminal()


def test_invalid_transitions_have_distinct_reasons(store: TaskStore) -> None:
    # 未领取（PENDING）直接完成 -> 非法流转，原因可区分
    task, _ = store.enqueue("k-pending")
    with pytest.raises(TaskRejected) as exc:
        store.complete(task.id, "nobody")
    assert exc.value.reason is RejectReason.NOT_PENDING_OR_UNCLAIMED

    claimed = store.acquire("owner")
    # 非持有者
    with pytest.raises(TaskRejected) as exc:
        store.complete(task.id, "intruder")
    assert exc.value.reason is RejectReason.LEASE_HELD_BY_OTHER

    # 不存在的任务
    with pytest.raises(TaskRejected) as exc:
        store.complete("missing", "owner")
    assert exc.value.reason is RejectReason.TASK_NOT_FOUND


def test_invalid_max_attempts(store: TaskStore) -> None:
    with pytest.raises(ValueError):
        store.enqueue("bad", max_attempts=0)
