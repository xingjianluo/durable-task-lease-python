"""失败重试：退避窗口、上限终态、过期版本写入拒绝。"""

import pytest

from task_store.clock import FakeClock
from task_store.store import TaskStore
from task_store.types import (
    LeaseResult,
    RejectReason,
    RetryResult,
    StateTransitionError,
    TaskStatus,
)


@pytest.fixture()
def store(tmp_path):
    clock = FakeClock(start=1000.0)
    s = TaskStore(
        str(tmp_path / "tasks.db"), clock,
        default_lease_seconds=100.0,
        default_max_attempts=2,
        default_backoff_base=10.0,
        default_backoff_factor=2.0,
    )
    yield s
    s.close()


def _claim(store, owner):
    outcome, lease = store.claim_next(owner)
    assert outcome in (LeaseResult.ACQUIRED, LeaseResult.EXPIRED_REACQUIRED)
    return lease


def test_backoff_blocks_claim_until_window_passes(store):
    store.enqueue("job-A")
    lease = _claim(store, "c1")
    res, task = store.fail(lease.task_id, "c1", lease.fencing_token, "boom")
    assert res is RetryResult.RETRY_SCHEDULED
    assert task.status is TaskStatus.PENDING
    # attempt_count 已在 claim 时记为 1；退避 = 10 * 2^(1-1) = 10s
    assert round(task.available_at - 1000.0, 6) == 10.0

    # 退避窗口内领取不到
    store._clock.advance(5)
    assert store.claim_next("c1") == (LeaseResult.EMPTY, None)
    # 窗口过后可领取，第二次尝试
    store._clock.advance(5.1)
    lease2 = _claim(store, "c1")
    assert lease2.fencing_token == lease.fencing_token + 1
    assert store.get(lease.task_id).attempt_count == 2


def test_retry_exhausted_enters_failed_terminal(store):
    store.enqueue("job-A")
    l1 = _claim(store, "c1")
    assert store.fail(l1.task_id, "c1", l1.fencing_token, "e1")[0] is RetryResult.RETRY_SCHEDULED
    store._clock.advance(20)
    l2 = _claim(store, "c1")
    res, task = store.fail(l2.task_id, "c1", l2.fencing_token, "e2")
    assert res is RetryResult.EXHAUSTED
    assert task.status is TaskStatus.FAILED
    # 终态不再被领取
    store._clock.advance(1000)
    assert store.claim_next("c1") == (LeaseResult.EMPTY, None)
    with pytest.raises(StateTransitionError) as ei:
        store.enqueue("job-A")
    assert ei.value.reason is RejectReason.ALREADY_TERMINAL


def test_stale_version_write_rejected(store):
    store.enqueue("job-A")
    l = _claim(store, "c1")
    snap = store.get(l.task_id)
    # 续期使 version +1，snap.version 过期
    res, _ = store.renew(l.task_id, "c1", l.fencing_token)
    assert res.name == "RENEWED"
    with pytest.raises(StateTransitionError) as ei:
        store.complete(l.task_id, "c1", l.fencing_token,
                       expected_version=snap.version)
    assert ei.value.reason is RejectReason.VERSION_CONFLICT
    # 不带版本号、租约仍有效 -> 正常接受
    assert store.complete(l.task_id, "c1", l.fencing_token).name == "ACCEPTED"


def test_claim_priority_oldest_available(store):
    """多个任务时按 available_at 最早者领取。"""
    store.enqueue("a")
    store._clock.advance(1)
    store.enqueue("b")
    _, l = store.claim_next("c")
    assert store.get(l.task_id).idempotency_key == "a"
