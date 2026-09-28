"""异常运行任务（过期/租约信息异常）的分类、恢复与隔离测试。"""
from __future__ import annotations

import sqlite3

import pytest

from tasklease import (
    RecoveryAction,
    RecoveryState,
    RejectReason,
    TaskRejected,
    TaskStatus,
    TaskStore,
)
from tasklease.recovery import RecoveryService

from ._logutil import log_events, log_task


def test_legacy_running_row_without_lease_is_corrupt(tmp_path, clock) -> None:
    db = tmp_path / "legacy-running.db"
    raw = sqlite3.connect(db)
    raw.execute("CREATE TABLE tasks (id TEXT PRIMARY KEY, status TEXT NOT NULL)")
    raw.executemany(
        "INSERT INTO tasks (id, status) VALUES (?, 'running')",
        [("run-no-owner",), ("run-no-exp",)],
    )
    raw.commit()
    raw.close()

    st = TaskStore(str(db), clock=clock)  # 触发平滑升级到 v3
    svc = RecoveryService(st)
    t = st.get("run-no-owner")
    assert t.status is TaskStatus.RUNNING
    assert svc.classify(t) is RecoveryState.CORRUPT

    report = svc.scan()
    assert report.scanned_running == 2
    assert report.corrupt == 2
    assert report.active == 0 and report.expired == 0
    log_task("旧库升级后的异常运行行", t)

    # 关键安全属性：异常行不会被正常领取流程抢走/误执行
    st.enqueue("normal-job")
    grant = st.acquire("w")
    assert grant is not None and grant.idempotency_key == "normal-job"
    st.close()


def test_recover_expired_only_touches_expired(store, clock) -> None:
    svc = RecoveryService(store)

    # 先把三个任务都领到手（此时租约都有效）
    store.enqueue("active-1")
    active = store.acquire("a", lease_seconds=100)
    store.enqueue("exp-1")
    expired = store.acquire("b", lease_seconds=10)
    store.enqueue("bad-1")
    bad = store.acquire("c", lease_seconds=100)

    clock.advance(50)  # exp-1 到期；active-1、bad-1 仍有效

    # 手工抹掉 bad-1 的凭证：租约信息异常（即便时间上还没到期也算 corrupt）
    with store._conn:  # type: ignore[attr-defined]
        store._conn.execute(  # type: ignore[attr-defined]
            "UPDATE tasks SET lease_token=NULL WHERE id=?", (bad.id,)
        )

    report = svc.recover_expired()
    assert report.scanned_running == 3
    assert report.expired == 1 and report.corrupt == 1 and report.active == 1
    # active 与 corrupt 都不被 recover_expired 触碰
    assert store.get(active.id).lease_owner == "a"
    assert store.get(bad.id).status is TaskStatus.RUNNING

    fixed = store.get(expired.id)
    assert fixed.status is TaskStatus.PENDING
    assert fixed.lease_owner is None and fixed.lease_token is None
    assert fixed.available_at == pytest.approx(clock.now)
    log_events(store._conn, expired.id)  # type: ignore[attr-defined]

    # 异常行交给 recover_all 隔离
    full = svc.recover_all()
    assert full.quarantined == 1
    assert store.get(bad.id).status is TaskStatus.QUARANTINED


def test_recover_expired_is_idempotent(store, clock) -> None:
    svc = RecoveryService(store)
    store.enqueue("exp-idem")
    store.acquire("b", lease_seconds=10)
    clock.advance(11)

    first = svc.recover_expired()
    assert first.recovered == 1
    # 再跑一遍：已无 RUNNING 行，零副作用、零计数
    second = svc.recover_expired()
    assert second.scanned_running == 0
    assert second.recovered == 0
    assert second.actions == []
    # 恢复出的任务可以被正常领取执行
    grant = store.acquire("w")
    assert grant is not None and grant.idempotency_key == "exp-idem"
    store.complete(grant.id, "w", lease_token=grant.lease_token)


def test_recover_all_quarantines_corrupt_and_allows_manual_requeue(
    tmp_path, clock
) -> None:
    db = tmp_path / "corrupt.db"
    raw = sqlite3.connect(db)
    raw.execute("CREATE TABLE tasks (id TEXT PRIMARY KEY, status TEXT NOT NULL)")
    raw.execute("INSERT INTO tasks (id, status) VALUES ('x', 'running')")
    raw.commit()
    raw.close()

    st = TaskStore(str(db), clock=clock)
    svc = RecoveryService(st)
    report = svc.recover_all()
    assert report.quarantined == 1
    q = st.get("x")
    assert q.status is TaskStatus.QUARANTINED
    assert report.actions[0][1] is RecoveryAction.QUARANTINED_CORRUPT
    log_task("异常行被隔离", q)

    # 隔离状态不能提交结果：明确的拒绝原因
    with pytest.raises(TaskRejected) as exc:
        st.complete("x", "nobody", lease_token="t" * 32)
    assert exc.value.reason is RejectReason.TASK_QUARANTINED
    # 隔离任务不会被领取
    assert st.acquire("w") is None

    # 幂等：再跑 recover_all 不会重复隔离/产生副作用
    again = svc.recover_all()
    assert again.scanned_running == 0 and again.quarantined == 0

    # 人工确认后放回 PENDING，可重新领取
    back = svc.requeue_quarantined("x")
    assert back.status is TaskStatus.PENDING
    grant = st.acquire("w")
    assert grant is not None and grant.id == "x"
    st.close()


def test_recover_all_can_requeue_corrupt_directly(tmp_path) -> None:
    db = tmp_path / "corrupt2.db"
    raw = sqlite3.connect(db)
    raw.execute("CREATE TABLE tasks (id TEXT PRIMARY KEY, status TEXT NOT NULL)")
    raw.execute("INSERT INTO tasks (id, status) VALUES ('y', 'running')")
    raw.commit()
    raw.close()

    st = TaskStore(str(db))
    svc = RecoveryService(st)
    report = svc.recover_all(requeue_corrupt=True)
    assert report.recovered == 1
    assert st.get("y").status is TaskStatus.PENDING
    grant = st.acquire("w")
    assert grant is not None and grant.id == "y"
    st.close()


def test_recovery_never_touches_active_lease(store, clock) -> None:
    svc = RecoveryService(store)
    store.enqueue("keep")
    grant = store.acquire("owner", lease_seconds=100)
    version_before = grant.version

    # 无论跑哪种恢复，有效租约都不受影响
    r1 = svc.recover_expired()
    r2 = svc.recover_all()
    assert r1.active == 1 and r2.active == 1
    held = store.get(grant.id)
    assert held.lease_owner == "owner"
    assert held.version == version_before
    assert held.lease_token == grant.lease_token

    # 恢复期间正常提交流程不被阻塞
    done = store.complete(grant.id, "owner", {"ok": 1},
                          lease_token=grant.lease_token)
    assert done.status is TaskStatus.SUCCEEDED


def test_manual_requeue_validates_state_and_version(store) -> None:
    svc = RecoveryService(store)
    task, _ = store.enqueue("pend")
    # 非 quarantined 不能走人工放回
    with pytest.raises(TaskRejected) as exc:
        svc.requeue_quarantined(task.id)
    assert exc.value.reason is RejectReason.NOT_PENDING_OR_UNCLAIMED
