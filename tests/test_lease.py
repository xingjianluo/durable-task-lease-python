"""并发领取、租约过期、主动释放与续期、执行凭证测试。"""
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
from .conftest import claim


def test_acquire_issues_per_execution_token(store: TaskStore) -> None:
    task, _ = store.enqueue("lease-1")
    first, tok1 = claim(store, "c1", lease_seconds=10)
    # 任务已被 c1 持有且租约未过期：其它消费者无法领取
    assert store.acquire("c2", lease_seconds=10) is None
    log_task("c1 持有租约", first)

    held = store.get(task.id)
    assert held.lease_owner == "c1"
    assert held.lease_token == tok1
    assert held.status.value == "running"

    # 释放后重新领取：凭证轮换
    assert store.release(task.id, "c1", tok1) is ReleaseOutcome.RELEASED
    second, tok2 = claim(store, "c1", lease_seconds=10)
    assert tok1 != tok2
    assert second.lease_token == tok2


def test_lease_expiry_allows_reacquire_with_clear_owner(store, clock) -> None:
    task, _ = store.enqueue("lease-exp")
    first, tok1 = claim(store, "c1", lease_seconds=10)
    log_task("首次领取", first)

    # 租约未到期不能被他人重新领取
    clock.advance(9)
    assert store.acquire("c2") is None

    # 租约到期后：旧持有者提交结果被拒，新持有者领取成功
    clock.advance(2)  # 超过到期时刻
    with pytest.raises(TaskRejected) as exc:
        store.complete(first.id, "c1", tok1, expected_version=first.version)
    assert exc.value.reason is RejectReason.LEASE_EXPIRED

    second, tok2 = claim(store, "c2", lease_seconds=10)
    assert second.lease_owner == "c2"
    assert second.attempt == first.attempt + 1
    assert tok1 != tok2
    log_task("租约过期后 c2 接管", second)

    # 接管后旧持有者再提交 -> 归属他人，拒绝原因可区分
    with pytest.raises(TaskRejected) as exc:
        store.complete(first.id, "c1", tok1)
    assert exc.value.reason is RejectReason.LEASE_HELD_BY_OTHER
    log_events(store._conn, task.id)  # type: ignore[attr-defined]


def test_same_consumer_name_old_token_cannot_submit_after_rotation(store, clock) -> None:
    """同名消费者在租约过期后重新领到同一任务：上一轮凭证立刻失效。"""
    task, _ = store.enqueue("same-name")
    first, tok1 = claim(store, "worker", lease_seconds=10)
    clock.advance(11)
    second, tok2 = claim(store, "worker", lease_seconds=10)
    assert second.id == first.id and tok1 != tok2

    # 旧一轮执行（同名、旧凭证）提交结果 -> invalid_token，不能覆盖新一轮产出
    with pytest.raises(TaskRejected) as exc:
        store.complete(task.id, "worker", tok1, {"from": "old-run"})
    assert exc.value.reason is RejectReason.INVALID_TOKEN

    with pytest.raises(TaskRejected) as exc:
        store.fail(task.id, "worker", tok1, "late failure")
    assert exc.value.reason is RejectReason.INVALID_TOKEN

    # 新一轮执行正常提交，产出不被旧执行覆盖
    done = store.complete(task.id, "worker", tok2, {"from": "new-run"})
    assert done.result == {"from": "new-run"}
    assert store.get_result(task.id)["result"] == {"from": "new-run"}
    log_events(store._conn, task.id)  # type: ignore[attr-defined]


def test_heartbeat_distinct_outcomes(store, clock) -> None:
    task, _ = store.enqueue("hb-1")
    claimed, tok = claim(store, "c1", lease_seconds=10)

    # 持有者续期成功
    assert store.heartbeat(task.id, "c1", tok, lease_seconds=20) is HeartbeatOutcome.RENEWED
    renewed = store.get(task.id)
    log_task("续期成功", renewed)
    assert renewed.lease_expires_at == claimed.lease_expires_at + 10
    assert renewed.version == claimed.version + 1

    # 凭证对不上 -> invalid_token（同名旧执行）
    assert store.heartbeat(task.id, "c1", "stale-token") is HeartbeatOutcome.INVALID_TOKEN

    # 非持有者续期 -> NOT_HELD（执行权归属明确）
    assert store.heartbeat(task.id, "intruder", tok) is HeartbeatOutcome.NOT_HELD

    # 租约过期后续期 -> EXPIRED
    clock.advance(100)
    assert store.heartbeat(task.id, "c1", tok) is HeartbeatOutcome.EXPIRED
    log_events(store._conn, task.id)  # type: ignore[attr-defined]


def test_release_distinct_outcomes(store, clock) -> None:
    task, _ = store.enqueue("rel-1")
    claimed, tok = claim(store, "c1", lease_seconds=10)

    # 非持有者释放 -> NOT_HELD
    assert store.release(task.id, "other", tok) is ReleaseOutcome.NOT_HELD
    # 凭证错误 -> INVALID_TOKEN，状态不被改写
    assert store.release(task.id, "c1", "wrong-token") is ReleaseOutcome.INVALID_TOKEN
    assert store.get(task.id).status.value == "running"

    # 持有者主动释放 -> RELEASED，任务立刻可被重新领取
    assert store.release(task.id, "c1", tok) is ReleaseOutcome.RELEASED
    released = store.get(task.id)
    assert released.status.value == "pending"
    assert released.lease_owner is None and released.lease_token is None
    log_task("主动释放后", released)
    again, tok3 = claim(store, "c3")
    assert again.lease_owner == "c3"

    # 旧凭证无法释放新一轮租约
    assert store.release(task.id, "c3", tok) is ReleaseOutcome.INVALID_TOKEN

    # 租约过期后的释放 -> ALREADY_EXPIRED，状态不被改写
    clock.advance(100)
    stale = store.get(task.id)
    outcome = store.release(task.id, "c3", tok3)
    assert outcome is ReleaseOutcome.ALREADY_EXPIRED
    after = store.get(task.id)
    assert after.lease_owner == stale.lease_owner == "c3"
    log_events(store._conn, task.id)  # type: ignore[attr-defined]


def test_release_returns_not_held_for_unknown_and_terminal(store) -> None:
    assert store.release("missing", "c1", "t") is ReleaseOutcome.NOT_HELD
    task, _ = store.enqueue("rel-term")
    claimed, tok = claim(store, "c1")
    store.complete(claimed.id, "c1", tok)
    assert store.release(task.id, "c1", tok) is ReleaseOutcome.NOT_HELD


def test_stale_lease_cannot_submit_failure(store, clock) -> None:
    task, _ = store.enqueue("stale-fail", max_attempts=3)
    claimed, tok = claim(store, "c1", lease_seconds=10)
    clock.advance(11)
    with pytest.raises(TaskRejected) as exc:
        store.fail(task.id, "c1", tok, "late error")
    assert exc.value.reason is RejectReason.LEASE_EXPIRED
    # 失败未被接受：状态仍为 running、attempt 不变
    untouched = store.get(task.id)
    assert untouched.attempt == claimed.attempt
    assert untouched.last_error is None
