"""历史异常运行任务的健康分类与恢复测试。

旧版本升级上来的 RUNNING 行可能缺 lease_owner / lease_expires_at，
导致永远卡住、再不会被领取。恢复入口要能区分：
仍被有效持有 / 租约已过期 / 租约信息异常；恢复可反复执行、
不误伤正常执行、不阻塞领取与提交。
"""
from __future__ import annotations

import sqlite3
import threading

import pytest

from tasklease import (
    LeaseHealth,
    RecoveryAction,
    RecoveryResult,
    RejectReason,
    TaskRejected,
    TaskStatus,
    TaskStore,
)

from ._logutil import log_events, log_task


def _break_lease(st: TaskStore, task_id: str, **fields: object) -> None:
    sets = ", ".join(f"{k}=?" for k in fields)
    params = list(fields.values()) + [task_id]
    st._conn.execute(f"UPDATE tasks SET {sets} WHERE id=?", params)  # type: ignore[attr-defined]
    st._conn.commit()  # type: ignore[attr-defined]


def test_health_classifies_three_running_states(store, clock) -> None:
    held, _ = store.enqueue("rc-held")
    g = store.acquire_grant("w", lease_seconds=100)
    assert g is not None and g.id == held.id
    assert store.inspect_lease_health(held.id) is LeaseHealth.HELD

    # EXPIRED：字段齐全但到期时间已过（独立 store，直接改库，避免过期行
    # 被下一次正常领取接管而干扰后续领取的确定性）
    st2 = TaskStore(":memory:", default_lease_seconds=10, clock=clock)
    expired, _ = st2.enqueue("rc-exp")
    g2 = st2.acquire_grant("w2", lease_seconds=100)
    assert g2 is not None and g2.id == expired.id
    _break_lease(st2, expired.id, lease_expires_at=clock.now - 1)
    assert st2.inspect_lease_health(expired.id) is LeaseHealth.EXPIRED

    # MALFORMED：租约信息缺字段
    st3 = TaskStore(":memory:", default_lease_seconds=10, clock=clock)
    malformed, _ = st3.enqueue("rc-malformed")
    g3 = st3.acquire_grant("w3", lease_seconds=100)
    assert g3 is not None and g3.id == malformed.id
    _break_lease(st3, malformed.id, lease_expires_at=None)
    assert st3.inspect_lease_health(malformed.id) is LeaseHealth.MALFORMED

    # 非运行中任务
    assert store.inspect_lease_health(held.id) is not LeaseHealth.NOT_RUNNING
    done, _ = store.enqueue("rc-done")
    gd = store.acquire_grant("w4", lease_seconds=100)
    assert gd is not None and gd.id == done.id
    store.complete(done.id, "w4", token=gd.token)
    assert store.inspect_lease_health(done.id) is LeaseHealth.NOT_RUNNING
    st2.close(); st3.close()


def test_malformed_rows_requeue_and_become_claimable(store) -> None:
    # 三条任务都领取到 running（同一时钟下按 id 排序领取，不假设具体归属），
    # 然后分别弄坏各自的租约字段。
    ids: list[str] = []
    for key in ("rc-m1", "rc-m2", "rc-m3"):
        t, _ = store.enqueue(key)
        ids.append(t.id)
        g = store.acquire_grant("w", lease_seconds=10)
        assert g is not None and g.id == t.id
    t1_id, t2_id, t3_id = ids
    _break_lease(store, t1_id, lease_expires_at=None)  # 缺到期时间
    _break_lease(store, t2_id, lease_owner=None)      # 缺持有者
    _break_lease(store, t3_id, lease_owner="")        # 空持有者

    report = store.recover_running(RecoveryAction.REQUEUE)
    assert report.scanned == 3
    assert report.recovered == 3
    assert {i.health for i in report.items} == {LeaseHealth.MALFORMED}
    assert {i.result for i in report.items} == {RecoveryResult.REQUEUED}

    for tid in ids:
        fresh = store.get(tid)
        assert fresh.status is TaskStatus.PENDING
        assert fresh.lease_owner is None and fresh.lease_token is None
        log_task("恢复为可领取", fresh)

    # 恢复后三条都能重新走完生命周期（领取顺序不做假设，按 id 集合校验）
    reclaimed: set[str] = set()
    for _ in range(3):
        grant = store.acquire_grant("repair", lease_seconds=100)
        assert grant is not None
        reclaimed.add(grant.id)
        store.complete(grant.id, "repair", {"fixed": True}, token=grant.token)
    assert reclaimed == set(ids)
    log_events(store._conn)  # type: ignore[attr-defined]


def test_expired_lease_requeued_but_held_skipped(store, clock) -> None:
    held, _ = store.enqueue("rc-keep")
    gh = store.acquire_grant("keep", lease_seconds=1000)
    assert gh is not None

    expired, _ = store.enqueue("rc-go")
    ge = store.acquire_grant("gone", lease_seconds=10)
    assert ge is not None
    clock.advance(11)

    report = store.recover_running(RecoveryAction.REQUEUE)
    by_id = {i.task_id: i for i in report.items}
    assert by_id[expired.id].result is RecoveryResult.REQUEUED
    assert by_id[expired.id].health is LeaseHealth.EXPIRED
    assert by_id[held.id].result is RecoveryResult.SKIPPED_HELD
    assert by_id[held.id].health is LeaseHealth.HELD

    # 有效持有的任务原封不动，仍可正常续期/提交
    assert store.get(held.id).lease_owner == "keep"
    assert store.heartbeat(held.id, "keep", token=gh.token).value == "renewed"
    store.complete(held.id, "keep", token=gh.token)


def test_manual_action_marks_dead_letter(store) -> None:
    t, _ = store.enqueue("rc-manual")
    g = store.acquire_grant("w", lease_seconds=10)
    assert g is not None
    _break_lease(store, t.id, lease_expires_at=None)

    report = store.recover_running(RecoveryAction.MANUAL, task_ids=[t.id])
    assert report.items[0].result is RecoveryResult.MARKED_MANUAL
    dl = store.get(t.id)
    assert dl.status is TaskStatus.DEAD_LETTER
    log_task("标记人工处理", dl)

    # dead_letter 不会被领取
    assert store.acquire_grant("anyone") is None
    # 重复入队被明确拒绝
    with pytest.raises(TaskRejected) as exc:
        store.enqueue("rc-manual")
    assert exc.value.reason is RejectReason.TASK_DEAD_LETTER
    # 也不能提交结果
    with pytest.raises(TaskRejected) as exc:
        store.complete(t.id, "w", token=g.token)
    assert exc.value.reason is RejectReason.TASK_DEAD_LETTER


def test_recovery_is_idempotent(store) -> None:
    t, _ = store.enqueue("rc-idem")
    g = store.acquire_grant("w", lease_seconds=10)
    assert g is not None
    _break_lease(store, t.id, lease_expires_at=None)

    first = store.recover_running(task_ids=[t.id])
    assert first.recovered == 1
    version_after = store.get(t.id).version

    # 再次执行：不再产生状态变更/版本递增，无额外副作用
    second = store.recover_running(task_ids=[t.id])
    assert second.recovered == 0
    assert second.items[0].result is RecoveryResult.SKIPPED_NOT_RUNNING
    assert store.get(t.id).version == version_after


def test_recovery_does_not_touch_unknown_task(store) -> None:
    with pytest.raises(TaskRejected) as exc:
        store.recover_running(task_ids=["missing"])
    assert exc.value.reason is RejectReason.TASK_NOT_FOUND


def test_legacy_v2_running_rows_with_null_lease_fields_recover(tmp_path) -> None:
    """模拟从旧版本升级的异常 RUNNING 行：有 owner 但缺到期时间等。"""
    db = tmp_path / "legacy_stuck.db"
    raw = sqlite3.connect(db)
    raw.execute("CREATE TABLE tasks (id TEXT PRIMARY KEY, status TEXT NOT NULL)")
    raw.executemany(
        "INSERT INTO tasks (id, status) VALUES (?, 'running')",
        [("stuck-1",), ("stuck-2",)],
    )
    raw.commit()
    raw.close()

    st = TaskStore(str(db))
    # 升级后异常行被识别为 MALFORMED 而不是被静默领取
    assert st.inspect_lease_health("stuck-1") is LeaseHealth.MALFORMED
    assert st.acquire("x") is None
    report = st.recover_running()
    assert report.recovered == 2
    grant = st.acquire_grant("rescue", lease_seconds=10)
    assert grant is not None and grant.id == "stuck-1"
    st.complete("stuck-1", "rescue", {"rescued": True}, token=grant.token)
    assert st.get_result("stuck-1").result == {"rescued": True}
    st.close()


def test_recovery_does_not_block_normal_acquire_commit(tmp_path) -> None:
    """恢复逐条小事务提交：恢复进行期间正常任务的领取/提交不被卡住。"""
    db = tmp_path / "recover_live.db"
    st = TaskStore(str(db))
    # 先放一条“领取后弄坏”的卡住任务，再放正常任务，保证首次领取确定性
    stuck, _ = st.enqueue("stuck-live")
    gs = st.acquire_grant("s", lease_seconds=10)
    assert gs is not None and gs.id == stuck.id
    _break_lease(st, stuck.id, lease_expires_at=None)
    for i in range(20):
        st.enqueue(f"live-{i}", {"i": i}, max_attempts=1)

    errors: list[Exception] = []
    stop = threading.Event()

    def recover_worker() -> None:
        for _ in range(10):
            st.recover_running()
            stop.wait(0.001)

    def claim_worker() -> None:
        done = 0
        while done < 20 and not stop.is_set():
            grant = st.acquire_grant("buzzy", lease_seconds=30)
            if grant is None:
                stop.wait(0.001)
                continue
            try:
                st.complete(grant.id, "buzzy", {"done": done}, token=grant.token)
                done += 1
            except TaskRejected as exc:  # pragma: no cover - 不允许发生
                errors.append(exc)

    t1 = threading.Thread(target=recover_worker)
    t2 = threading.Thread(target=claim_worker)
    t1.start(); t2.start()
    t2.join(timeout=15); stop.set(); t1.join(timeout=5)

    assert not errors, errors
    # 卡住任务已恢复且能被重新领取
    assert st.get(stuck.id).status is TaskStatus.PENDING
    g = st.acquire_grant("final", lease_seconds=10)
    assert g is not None and g.id == stuck.id
    st.complete(stuck.id, "final", token=g.token)
    st.close()
