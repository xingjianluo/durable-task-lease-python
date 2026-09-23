"""并发领取、租约过期、主动释放与续期测试。"""
from __future__ import annotations

import pytest

from tasklease import (
    HeartbeatOutcome,
    ReleaseOutcome,
    RejectReason,
    TaskRejected,
    TaskStore,
)

from ._logutil import log_events, log_task


def test_only_one_consumer_holds_task(store: TaskStore) -> None:
    task, _ = store.enqueue("lease-1")
    first = store.acquire("c1", lease_seconds=10)
    # 任务已被 c1 持有且租约未过期：其它消费者无法领取
    second = store.acquire("c2", lease_seconds=10)
    assert first is not None and second is None
    log_task("c1 持有租约", first)

    held = store.get(task.id)
    assert held.lease_owner == "c1"
    assert held.status.value == "running"


def test_lease_expiry_allows_reacquire_with_clear_owner(store, clock) -> None:
    task, _ = store.enqueue("lease-exp")
    first = store.acquire("c1", lease_seconds=10)
    assert first is not None
    log_task("首次领取", first)

    # 租约未到期不能被他人重新领取
    clock.advance(9)
    assert store.acquire("c2") is None

    # 租约到期后：旧持有者提交结果被拒，新持有者领取成功
    clock.advance(2)  # 超过到期时刻
    with pytest.raises(TaskRejected) as exc:
        store.complete(first.id, "c1", expected_version=first.version)
    assert exc.value.reason is RejectReason.LEASE_EXPIRED

    second = store.acquire("c2", lease_seconds=10)
    assert second is not None
    assert second.lease_owner == "c2"
    assert second.attempt == first.attempt + 1
    log_task("租约过期后 c2 接管", second)

    # 接管后旧持有者再提交 -> 归属他人，拒绝原因可区分
    with pytest.raises(TaskRejected) as exc:
        store.complete(first.id, "c1")
    assert exc.value.reason is RejectReason.LEASE_HELD_BY_OTHER
    log_events(store._conn, task.id)  # type: ignore[attr-defined]


def test_heartbeat_three_distinct_outcomes(store, clock) -> None:
    task, _ = store.enqueue("hb-1")
    claimed = store.acquire("c1", lease_seconds=10)
    assert claimed is not None

    # 持有者续期成功
    assert store.heartbeat(task.id, "c1", lease_seconds=20) is HeartbeatOutcome.RENEWED
    renewed = store.get(task.id)
    log_task("续期成功", renewed)
    assert renewed.lease_expires_at == claimed.lease_expires_at + 10
    assert renewed.version == claimed.version + 1

    # 非持有者续期 -> NOT_HELD（执行权归属明确）
    assert store.heartbeat(task.id, "intruder") is HeartbeatOutcome.NOT_HELD

    # 租约过期后续期 -> EXPIRED
    clock.advance(100)
    assert store.heartbeat(task.id, "c1") is HeartbeatOutcome.EXPIRED
    log_events(store._conn, task.id)  # type: ignore[attr-defined]


def test_release_three_distinct_outcomes(store, clock) -> None:
    task, _ = store.enqueue("rel-1")
    claimed = store.acquire("c1", lease_seconds=10)
    assert claimed is not None

    # 非持有者释放 -> NOT_HELD
    assert store.release(task.id, "other") is ReleaseOutcome.NOT_HELD

    # 持有者主动释放 -> RELEASED，任务立刻可被重新领取
    assert store.release(task.id, "c1") is ReleaseOutcome.RELEASED
    released = store.get(task.id)
    assert released.status.value == "pending"
    assert released.lease_owner is None
    log_task("主动释放后", released)
    again = store.acquire("c3")
    assert again is not None and again.lease_owner == "c3"

    # 租约过期后的释放 -> ALREADY_EXPIRED，状态不被改写
    clock.advance(100)
    stale = store.get(task.id)
    outcome = store.release(task.id, "c3")
    assert outcome is ReleaseOutcome.ALREADY_EXPIRED
    after = store.get(task.id)
    assert after.lease_owner == stale.lease_owner == "c3"
    log_events(store._conn, task.id)  # type: ignore[attr-defined]


def test_release_returns_not_held_for_unknown_and_terminal(store) -> None:
    assert store.release("missing", "c1") is ReleaseOutcome.NOT_HELD
    task, _ = store.enqueue("rel-term")
    claimed = store.acquire("c1")
    store.complete(claimed.id, "c1")
    assert store.release(task.id, "c1") is ReleaseOutcome.NOT_HELD


def test_stale_lease_cannot_submit_failure(store, clock) -> None:
    task, _ = store.enqueue("stale-fail", max_attempts=3)
    claimed = store.acquire("c1", lease_seconds=10)
    clock.advance(11)
    with pytest.raises(TaskRejected) as exc:
        store.fail(task.id, "c1", "late error")
    assert exc.value.reason is RejectReason.LEASE_EXPIRED
    # 失败未被接受：状态仍为 running、attempt 不变
    untouched = store.get(task.id)
    assert untouched.attempt == claimed.attempt
    assert untouched.last_error is None
