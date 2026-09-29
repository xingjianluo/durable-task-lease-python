"""执行凭证（fence token）测试。

每次领取都会签发只属于本次执行的凭证；complete/fail/heartbeat/release
必须回传它。租约轮换（重新领取、释放、恢复）后旧凭证立即失效，
且同名消费者重新领取也无法用旧凭证覆盖新一轮产出。
"""
from __future__ import annotations

import pytest

from tasklease import (
    HeartbeatOutcome,
    LeaseGrant,
    ReleaseOutcome,
    RejectReason,
    TaskRejected,
    TaskStatus,
    TaskStore,
)

from ._logutil import log_events, log_task


def test_acquire_grant_returns_unique_token(store: TaskStore) -> None:
    task, _ = store.enqueue("ft-1")
    grant = store.acquire_grant("c1", lease_seconds=10)
    assert isinstance(grant, LeaseGrant)
    assert grant is not None and grant.token
    assert grant.task.id == task.id
    log_task("领取回执含凭证", grant.task)


def test_token_required_ops_succeed_with_current_token(store, clock) -> None:
    task, _ = store.enqueue("ft-2", max_attempts=3)
    grant = store.acquire_grant("c1", lease_seconds=100)
    assert grant is not None

    assert store.heartbeat(task.id, "c1", lease_seconds=20, token=grant.token) is (
        HeartbeatOutcome.RENEWED
    )
    # 主动释放需凭证；释放后任务可重新领取
    assert store.release(task.id, "c1", token=grant.token) is ReleaseOutcome.RELEASED

    grant2 = store.acquire_grant("c1", lease_seconds=100)
    assert grant2 is not None and grant2.token != grant.token
    failed = store.fail(
        task.id, "c1", "transient", backoff_seconds=0, token=grant2.token
    )
    assert failed.status is TaskStatus.PENDING


def test_old_token_rejected_after_lease_rotation(store, clock) -> None:
    """核心防覆盖：旧凭证在租约轮换那一刻起立即失效。"""
    task, _ = store.enqueue("ft-rot")
    first = store.acquire_grant("worker", lease_seconds=10)
    assert first is not None
    old_token = first.token

    # 租约过期、被另一个消费者接管（凭证轮换）
    clock.advance(11)
    second = store.acquire_grant("other", lease_seconds=10)
    assert second is not None and second.token != old_token

    # 旧执行用旧凭证提交成功结果 -> 明确拒绝：已被别人接管
    with pytest.raises(TaskRejected) as exc:
        store.complete(task.id, "worker", {"late": True}, token=old_token)
    assert exc.value.reason is RejectReason.LEASE_HELD_BY_OTHER

    # 旧执行上报失败同样被拒
    with pytest.raises(TaskRejected) as exc:
        store.fail(task.id, "worker", "late", token=old_token)
    assert exc.value.reason is RejectReason.LEASE_HELD_BY_OTHER

    # 旧凭证续期/释放也不被接受：NOT_HELD（owner 已不是自己）
    assert store.heartbeat(task.id, "worker", token=old_token) is (
        HeartbeatOutcome.NOT_HELD
    )
    assert store.release(task.id, "worker", token=old_token) is (
        ReleaseOutcome.NOT_HELD
    )

    # 新执行的产出不被覆盖
    done = store.complete(task.id, "other", {"final": 1}, token=second.token)
    assert done.status is TaskStatus.SUCCEEDED
    assert store.get_result(task.id).result == {"final": 1}
    log_events(store._conn, task.id)  # type: ignore[attr-defined]


def test_same_consumer_reacquire_old_token_cannot_overwrite(store, clock) -> None:
    """同名消费者在租约过期后重新领到同一任务：上一轮执行仍持旧凭证，
    不得提交，更不能覆盖新一轮执行的产出。"""
    task, _ = store.enqueue("ft-same-name")
    first = store.acquire_grant("dup-worker", lease_seconds=10)
    assert first is not None
    old_token = first.token

    clock.advance(11)
    second = store.acquire_grant("dup-worker", lease_seconds=10)
    assert second is not None
    assert second.task.id == task.id
    assert second.token != old_token
    log_task("同名消费者第二轮领取", second.task)

    # owner 仍然是 dup-worker、租约也有效，但凭证对不上 -> 明确区分于过期/接管
    with pytest.raises(TaskRejected) as exc:
        store.complete(task.id, "dup-worker", {"stale": True}, token=old_token)
    assert exc.value.reason is RejectReason.LEASE_TOKEN_MISMATCH
    with pytest.raises(TaskRejected) as exc:
        store.fail(task.id, "dup-worker", "stale", token=old_token)
    assert exc.value.reason is RejectReason.LEASE_TOKEN_MISMATCH

    # 续期/释放同样按“凭证对不上”拒绝
    assert store.heartbeat(task.id, "dup-worker", token=old_token) is (
        HeartbeatOutcome.INVALID_TOKEN
    )
    assert store.release(task.id, "dup-worker", token=old_token) is (
        ReleaseOutcome.INVALID_TOKEN
    )

    # 第二轮正常提交成功，结果是新一轮产出
    done = store.complete(task.id, "dup-worker", {"round": 2}, token=second.token)
    assert done.status is TaskStatus.SUCCEEDED
    assert store.get_result(task.id).result == {"round": 2}
    log_events(store._conn, task.id)  # type: ignore[attr-defined]


def test_distinct_rejections_expired_vs_other_vs_mismatch(store, clock) -> None:
    """三种“丢执行权”结论必须能分开：过期 / 被接管 / 凭证对不上。"""
    task, _ = store.enqueue("ft-distinct")
    grant = store.acquire_grant("a", lease_seconds=10)
    assert grant is not None

    # 1) 租约已过期（尚未被接管）
    clock.advance(11)
    with pytest.raises(TaskRejected) as exc:
        store.complete(task.id, "a", token=grant.token)
    assert exc.value.reason is RejectReason.LEASE_EXPIRED
    assert store.heartbeat(task.id, "a", token=grant.token) is (
        HeartbeatOutcome.EXPIRED
    )

    # 2) 过期后被别人接管
    takeover = store.acquire_grant("b", lease_seconds=10)
    assert takeover is not None
    with pytest.raises(TaskRejected) as exc:
        store.complete(task.id, "a", token=grant.token)
    assert exc.value.reason is RejectReason.LEASE_HELD_BY_OTHER

    # 3) 当前持有者却拿错凭证
    with pytest.raises(TaskRejected) as exc:
        store.complete(task.id, "b", token="not-the-current-token")
    assert exc.value.reason is RejectReason.LEASE_TOKEN_MISMATCH

    # 当前有效租约的正确凭证成功
    assert store.complete(task.id, "b", token=takeover.token).is_terminal()


def test_token_cleared_when_lease_ends(store, clock) -> None:
    """complete/fail/release 终结或交出租约后凭证清空，任何旧 token 都失效。"""
    task, _ = store.enqueue("ft-clear", max_attempts=2)
    grant = store.acquire_grant("c1", lease_seconds=100)
    assert grant is not None
    store.fail(task.id, "c1", "retry", backoff_seconds=0, token=grant.token)
    row = store._conn.execute(  # type: ignore[attr-defined]
        "SELECT lease_token, status FROM tasks WHERE id=?", (task.id,)
    ).fetchone()
    assert row[0] is None and row[1] == "pending"

    grant2 = store.acquire_grant("c2", lease_seconds=100)
    assert grant2 is not None
    store.complete(task.id, "c2", {"ok": True}, token=grant2.token)
    row = store._conn.execute(  # type: ignore[attr-defined]
        "SELECT lease_token, status FROM tasks WHERE id=?", (task.id,)
    ).fetchone()
    assert row[0] is None and row[1] == "succeeded"


def test_garbage_token_never_matches(store) -> None:
    task, _ = store.enqueue("ft-garbage")
    store.acquire_grant("c1", lease_seconds=100)
    with pytest.raises(TaskRejected) as exc:
        store.complete(task.id, "c1", token="")
    assert exc.value.reason is RejectReason.LEASE_TOKEN_MISMATCH


def test_token_invalidation_across_separate_connections(tmp_path) -> None:
    """跨进程模拟：不同连接（不同 TaskStore）之间租约轮换后旧凭证立即失效。"""
    db = tmp_path / "fence_xproc.db"
    boot = TaskStore(str(db), default_lease_seconds=60)
    t, _ = boot.enqueue("fence-xproc", {"v": 1})
    boot.close()

    s1 = TaskStore(str(db), default_lease_seconds=1)
    g1 = s1.acquire_grant("worker-x", lease_seconds=1)
    assert g1 is not None and g1.id == t.id
    s1.close()

    # 等待真实租约过期后，由另一个连接接管
    import time
    time.sleep(1.2)
    s2 = TaskStore(str(db), default_lease_seconds=60)
    g2 = s2.acquire_grant("worker-x", lease_seconds=60)  # 同名消费者
    assert g2 is not None and g2.id == t.id
    assert g2.token != g1.token

    # 旧连接重开，用旧凭证提交 -> 必须被拒（owner 同名但凭证不匹配）
    s1b = TaskStore(str(db), default_lease_seconds=60)
    with pytest.raises(TaskRejected) as exc:
        s1b.complete(t.id, "worker-x", {"stale": True}, token=g1.token)
    assert exc.value.reason is RejectReason.LEASE_TOKEN_MISMATCH
    s1b.close()

    # 新连接用新凭证成功，旧产出无法覆盖
    done = s2.complete(t.id, "worker-x", {"fresh": True}, token=g2.token)
    assert done.status is TaskStatus.SUCCEEDED
    assert s2.get_result(t.id).result == {"fresh": True}
    s2.close()
