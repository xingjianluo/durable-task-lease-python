"""失败重试、终态与乐观版本冲突、旧数据平滑升级测试。"""
from __future__ import annotations

import sqlite3

import pytest

from tasklease import RejectReason, TaskRejected, TaskStatus, TaskStore

from ._logutil import log_events, log_task


def test_retryable_failure_requeues_with_backoff(store, clock) -> None:
    task, _ = store.enqueue("retry-1", max_attempts=3)
    claimed = store.acquire("w", lease_seconds=100)
    assert claimed is not None and claimed.attempt == 1

    failed = store.fail(task.id, "w", "transient", backoff_seconds=30)
    assert failed.status is TaskStatus.PENDING
    assert failed.attempt == 1
    assert failed.available_at == pytest.approx(clock.now + 30)
    assert failed.lease_owner is None
    log_task("可重试失败，退避30s", failed)

    # 退避窗口内不可领取
    clock.advance(29)
    assert store.acquire("w2") is None
    clock.advance(2)  # 越过退避点
    reclaimed = store.acquire("w2")
    assert reclaimed is not None and reclaimed.attempt == 2
    log_task("退避结束重新领取", reclaimed)
    log_events(store._conn, task.id)  # type: ignore[attr-defined]


def test_exponential_backoff_default(store, clock) -> None:
    # backoff_base=1.0：默认退避为 1 * 2**(attempt-1)
    task, _ = store.enqueue("retry-bo", max_attempts=5)
    t1 = store.acquire("w", lease_seconds=100)
    f1 = store.fail(task.id, "w", "e")  # attempt=1 -> 1s
    assert f1.available_at == pytest.approx(clock.now + 1.0)
    clock.advance(1.0)
    t2 = store.acquire("w", lease_seconds=100)
    f2 = store.fail(task.id, "w", "e")  # attempt=2 -> 2s
    assert f2.available_at == pytest.approx(clock.now + 2.0)


def test_retry_exhaustion_enters_terminal(store, clock) -> None:
    task, _ = store.enqueue("retry-max", max_attempts=2)
    t1 = store.acquire("w", lease_seconds=100)
    f1 = store.fail(task.id, "w", "e1", backoff_seconds=0)
    assert f1.status is TaskStatus.PENDING
    t2 = store.acquire("w", lease_seconds=100)
    assert t2.attempt == 2
    f2 = store.fail(task.id, "w", "e2")
    assert f2.status is TaskStatus.FAILED
    assert f2.is_terminal()
    log_task("重试耗尽进入终态", f2)

    # 终态任务不再被领取
    assert store.acquire("w") is None
    # 终态提交被拒
    with pytest.raises(TaskRejected) as exc:
        store.complete(task.id, "w")
    assert exc.value.reason is RejectReason.TASK_TERMINAL


def test_succeeded_terminal_not_acquired(store) -> None:
    task, _ = store.enqueue("done-1")
    claimed = store.acquire("w")
    done = store.complete(claimed.id, "w", {"ok": True})
    assert done.status is TaskStatus.SUCCEEDED
    assert store.acquire("w") is None


def test_stale_version_write_rejected(store, clock) -> None:
    task, _ = store.enqueue("ver-1")
    t1 = store.acquire("c1", lease_seconds=10)
    # 另一消费者等租约过期后接管，版本被推进
    clock.advance(11)
    t2 = store.acquire("c2", lease_seconds=10)
    assert t2.version > t1.version

    # c1 拿着旧版本尝试完成 -> 版本冲突，而不是静默生效
    with pytest.raises(TaskRejected) as exc:
        store.complete(task.id, "c1", result={"x": 1}, expected_version=t1.version)
    assert exc.value.reason is RejectReason.LEASE_HELD_BY_OTHER  # 先判定租约归属

    # 持有者用过期版本提交 -> 明确的版本冲突拒绝
    clock.advance(11)
    t3 = store.acquire("c3", lease_seconds=10)
    # 模拟客户端基于 t2 的版本写回
    with pytest.raises(TaskRejected) as exc:
        store.complete(task.id, "c3", expected_version=t2.version)
    assert exc.value.reason is RejectReason.VERSION_CONFLICT

    # 基于当前版本写入成功
    ok = store.complete(task.id, "c3", expected_version=t3.version)
    assert ok.status is TaskStatus.SUCCEEDED
    log_task("版本对齐后写入成功", ok)
    log_events(store._conn, task.id)  # type: ignore[attr-defined]


def test_concurrent_overwrite_prevented(store, clock) -> None:
    # 同一次领取后，版本只能被成功消费一次：重复的条件更新 rowcount=0
    task, _ = store.enqueue("ver-2")
    claimed = store.acquire("c1", lease_seconds=10)
    ok = store.complete(task.id, "c1", expected_version=claimed.version)
    assert ok.version == claimed.version + 1
    with pytest.raises(TaskRejected) as exc:
        store.complete(task.id, "c1", expected_version=claimed.version)
    assert exc.value.reason is RejectReason.TASK_TERMINAL


def test_legacy_database_upgrades_and_stays_readable(tmp_path) -> None:
    db = tmp_path / "legacy.db"
    raw = sqlite3.connect(db)
    # 模拟历史版本：极简 tasks 表 + 一行旧数据
    raw.execute("CREATE TABLE tasks (id TEXT PRIMARY KEY, status TEXT NOT NULL)")
    raw.execute("INSERT INTO tasks (id, status) VALUES ('legacy-1', 'pending')")
    raw.commit()
    raw.close()

    st = TaskStore(str(db))
    upgraded = st.get("legacy-1")
    assert upgraded.status is TaskStatus.PENDING
    assert upgraded.idempotency_key == ""
    assert upgraded.payload == {}
    assert upgraded.attempt == 0 and upgraded.version == 0
    log_task("旧数据升级后可读", upgraded)

    # 旧数据参与正常生命周期
    claim = st.acquire("legacy-worker")
    assert claim is not None and claim.id == "legacy-1"
    done = st.complete("legacy-1", "legacy-worker", expected_version=claim.version)
    assert done.status is TaskStatus.SUCCEEDED
    st.close()


def test_legacy_rows_with_duplicate_empty_keys_do_not_block_index(tmp_path) -> None:
    # 空 idempotency_key 的旧行可能有多条；唯一索引只约束非空键
    db = tmp_path / "legacy2.db"
    raw = sqlite3.connect(db)
    raw.execute("CREATE TABLE tasks (id TEXT PRIMARY KEY, status TEXT NOT NULL)")
    raw.executemany(
        "INSERT INTO tasks (id, status) VALUES (?, 'pending')",
        [("a",), ("b",), ("c",)],
    )
    raw.commit()
    raw.close()

    st = TaskStore(str(db))
    assert st.get("a").id == "a"
    assert st.get("c").id == "c"
    st.close()
