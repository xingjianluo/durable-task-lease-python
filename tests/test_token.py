"""执行凭证（lease_token）测试：凭证只对本次租约世代有效。"""
from __future__ import annotations

import pytest

from tasklease import (
    HeartbeatOutcome,
    LeaseGrant,
    RejectReason,
    ReleaseOutcome,
    TaskRejected,
    TaskStore,
)

from ._logutil import log_events, log_task


def test_acquire_returns_grant_with_opaque_token(store: TaskStore) -> None:
    store.enqueue("tok-1")
    grant = store.acquire("c1", lease_seconds=10)
    assert isinstance(grant, LeaseGrant)
    assert grant is not None
    assert grant.lease_token
    assert len(grant.lease_token) >= 16
    log_task("领取并获得凭证", grant)

    # 持正确凭证提交成功
    done = store.complete(grant.id, "c1", {"out": 1}, lease_token=grant.lease_token)
    assert done.status.value == "succeeded"


def test_missing_token_rejected_distinctly(store: TaskStore) -> None:
    task, _ = store.enqueue("tok-missing")
    store.acquire("c1", lease_seconds=10)

    with pytest.raises(TaskRejected) as exc:
        store.complete(task.id, "c1", lease_token="")
    assert exc.value.reason is RejectReason.TOKEN_MISSING

    with pytest.raises(TaskRejected) as exc:
        store.fail(task.id, "c1", "boom", lease_token="")
    assert exc.value.reason is RejectReason.TOKEN_MISSING
    log_events(store._conn, task.id)  # type: ignore[attr-defined]


def test_garbage_token_rejected(store: TaskStore) -> None:
    task, _ = store.enqueue("tok-garbage")
    grant = store.acquire("c1", lease_seconds=10)
    with pytest.raises(TaskRejected) as exc:
        store.complete(task.id, "c1", lease_token="deadbeef" * 4)
    assert exc.value.reason is RejectReason.TOKEN_MISMATCH
    assert grant.lease_token != "deadbeef" * 4


def test_token_rotated_after_expiry_old_token_invalid(store, clock) -> None:
    task, _ = store.enqueue("tok-rotate")
    first = store.acquire("c1", lease_seconds=10)
    assert isinstance(first, LeaseGrant)
    first_token = first.lease_token
    log_task("第一次领取", first)

    # 租约未过期：旧凭证当然有效（持有者续期/提交都成功）
    assert store.heartbeat(task.id, "c1", lease_seconds=10,
                           lease_token=first_token) is HeartbeatOutcome.RENEWED

    clock.advance(100)  # 租约过期，被另一个消费者接管 -> 新凭证
    second = store.acquire("c2", lease_seconds=10)
    assert isinstance(second, LeaseGrant)
    assert second.lease_token != first_token
    log_task("过期后 c2 接管，新凭证", second)

    # 旧持有者 c1 已不再是持有者：续期/释放按归属判定 not_held
    assert store.heartbeat(
        task.id, "c1", lease_seconds=10, lease_token=first_token
    ) is HeartbeatOutcome.NOT_HELD
    assert store.release(
        task.id, "c1", lease_token=first_token
    ) is ReleaseOutcome.NOT_HELD
    with pytest.raises(TaskRejected) as exc:
        store.complete(task.id, "c1", {"stale": True},
                       lease_token=first_token)
    assert exc.value.reason is RejectReason.LEASE_HELD_BY_OTHER

    # 当前持有者用新凭证成功完成，旧执行没有覆盖结果
    done = store.complete(task.id, "c2", {"winner": "c2"},
                          lease_token=second.lease_token)
    assert done.status.value == "succeeded"
    result = store.result_row(task.id)
    assert result["result"] == '{"winner":"c2"}'
    log_events(store._conn, task.id)  # type: ignore[attr-defined]


def test_same_consumer_name_old_token_cannot_submit_after_reacquire(
    store, clock
) -> None:
    """同名消费者在租约过期后重新领到同一任务：上次的凭证必须立即作废。"""
    task, _ = store.enqueue("tok-same-name")
    first = store.acquire("same-worker", lease_seconds=10)
    assert isinstance(first, LeaseGrant)
    old_token = first.lease_token

    clock.advance(11)
    second = store.acquire("same-worker", lease_seconds=10)
    assert isinstance(second, LeaseGrant)
    assert second.attempt == first.attempt + 1
    assert second.lease_token != old_token
    log_task("同名消费者重新领取", second)

    # 持有者名字相同，但旧凭证属于上一世代：不能提交，也不能覆盖后来的执行
    with pytest.raises(TaskRejected) as exc:
        store.complete(task.id, "same-worker", {"old": True},
                       lease_token=old_token)
    assert exc.value.reason is RejectReason.TOKEN_MISMATCH
    # 拒绝信息可解释：能看出是凭证轮换
    assert "凭证" in str(exc.value)

    with pytest.raises(TaskRejected) as exc:
        store.fail(task.id, "same-worker", "old failure",
                   lease_token=old_token)
    assert exc.value.reason is RejectReason.TOKEN_MISMATCH

    # 续期、释放同样拒绝旧凭证（同名但不是本次租约的凭证）
    assert store.heartbeat(task.id, "same-worker", lease_seconds=10,
                           lease_token=old_token) is HeartbeatOutcome.TOKEN_MISMATCH
    assert store.release(task.id, "same-worker",
                         lease_token=old_token) is ReleaseOutcome.TOKEN_MISMATCH
    untouched = store.get(task.id)
    assert untouched.status.value == "running"
    assert untouched.lease_owner == "same-worker"

    # 新凭证正常上报失败也可以
    back = store.fail(task.id, "same-worker", "new failure",
                      backoff_seconds=0, lease_token=second.lease_token)
    assert back.status.value == "pending"
    log_events(store._conn, task.id)  # type: ignore[attr-defined]


def test_release_and_heartbeat_require_current_token(store) -> None:
    task, _ = store.enqueue("tok-hb-rel")
    grant = store.acquire("c1", lease_seconds=10)

    # 错误凭证续期/释放：明确的 token_mismatch，状态不改写
    assert store.heartbeat(task.id, "c1", lease_seconds=10,
                           lease_token="x" * 32) is HeartbeatOutcome.TOKEN_MISMATCH
    assert store.release(task.id, "c1",
                         lease_token="x" * 32) is ReleaseOutcome.TOKEN_MISMATCH
    still = store.get(task.id)
    assert still.lease_owner == "c1"
    assert still.version == grant.version  # 被拒操作不推进版本

    # 正确凭证释放成功
    assert store.release(
        task.id, "c1", lease_token=grant.lease_token
    ) is ReleaseOutcome.RELEASED
    assert store.get(task.id).status.value == "pending"


def test_token_consumed_on_release_and_reacquire(store) -> None:
    task, _ = store.enqueue("tok-reacq")
    first = store.acquire("c1", lease_seconds=10)
    assert store.release(task.id, "c1",
                         lease_token=first.lease_token) is ReleaseOutcome.RELEASED

    second = store.acquire("c1", lease_seconds=10)
    assert isinstance(second, LeaseGrant)
    assert second.lease_token != first.lease_token
    # 释放前那一世代的凭证不能操作新租约
    with pytest.raises(TaskRejected) as exc:
        store.complete(task.id, "c1", lease_token=first.lease_token)
    assert exc.value.reason is RejectReason.TOKEN_MISMATCH
