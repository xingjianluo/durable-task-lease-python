"""基础状态机测试：幂等入队、租约三态、过期提交拒绝、终态拒绝。"""

import pytest

from task_store.clock import FakeClock
from task_store.store import TaskStore
from task_store.types import (
    CompleteResult,
    EnqueueOutcome,
    LeaseResult,
    ReleaseResult,
    RenewResult,
    RejectReason,
    StateTransitionError,
    TaskStatus,
)


@pytest.fixture()
def store(tmp_path):
    clock = FakeClock(start=1000.0)
    s = TaskStore(
        str(tmp_path / "tasks.db"), clock,
        default_lease_seconds=10.0,
        default_max_attempts=2,
        default_backoff_base=1.0,
        default_backoff_factor=2.0,
    )
    yield s
    s.close()


def test_idempotent_enqueue_dedupes_pending(store):
    r1 = store.enqueue("job-A", "payload-1")
    r2 = store.enqueue("job-A", "payload-1")
    r3 = store.enqueue("job-A", "payload-2")  # 内容不同也收敛
    assert r1.outcome is EnqueueOutcome.CREATED
    assert r2.outcome is EnqueueOutcome.DEDUPED_PENDING
    assert r3.outcome is EnqueueOutcome.DEDUPED_PENDING
    assert r1.task.id == r2.task.id == r3.task.id
    assert len(store.list_tasks()) == 1


def test_enqueue_rejected_while_running_and_terminal(store):
    store.enqueue("job-A")
    outcome, lease = store.claim_next("consumer-1")
    assert outcome is LeaseResult.ACQUIRED and lease is not None

    with pytest.raises(StateTransitionError) as ei:
        store.enqueue("job-A")
    assert ei.value.reason is RejectReason.ALREADY_RUNNING

    store.complete(lease.task_id, "consumer-1", lease.fencing_token)
    with pytest.raises(StateTransitionError) as ei:
        store.enqueue("job-A")
    assert ei.value.reason is RejectReason.ALREADY_TERMINAL
    assert len(store.list_tasks()) == 1


def test_claim_is_exclusive_and_renew_release_distinguishable(store):
    store.enqueue("job-A")
    _, lease = store.claim_next("c1")
    assert store.claim_next("c2") == (LeaseResult.EMPTY, None)  # 同一任务不被第二者拿走

    # 别的消费者续期/释放 -> HELD_BY_OTHER
    res, _ = store.renew(lease.task_id, "c2", lease.fencing_token)
    assert res is RenewResult.HELD_BY_OTHER
    assert store.release(lease.task_id, "c2", lease.fencing_token) is ReleaseResult.HELD_BY_OTHER

    # 持有者续期成功
    res, new_lease = store.renew(lease.task_id, "c1", lease.fencing_token, lease_seconds=20)
    assert res is RenewResult.RENEWED and new_lease is not None
    assert new_lease.expires_at > lease.expires_at

    # 持有者主动释放 -> RELEASED，任务可再领取
    assert store.release(lease.task_id, "c1", lease.fencing_token) is ReleaseResult.RELEASED
    # 旧租约释放后再释放 -> NOT_HELD
    assert store.release(lease.task_id, "c1", lease.fencing_token) is ReleaseResult.NOT_HELD
    _, lease2 = store.claim_next("c2")
    assert lease2 is not None and lease2.fencing_token > lease.fencing_token


def test_expired_lease_reacquired_and_stale_owner_rejected(store):
    store.enqueue("job-A")
    _, lease1 = store.claim_next("c1")
    # c1 持有过期租约：续期 -> LEASE_EXPIRED；释放 -> LEASE_EXPIRED
    # 先用未过期状态做一次续期基线，再推进时间到过期
    # 直接推进到过期
    store._clock.advance(10.1)
    res, _ = store.renew(lease1.task_id, "c1", lease1.fencing_token)
    assert res is RenewResult.LEASE_EXPIRED
    assert store.release(lease1.task_id, "c1", lease1.fencing_token) is ReleaseResult.LEASE_EXPIRED

    # 持过期租约提交结果 -> 拒绝
    assert store.complete(lease1.task_id, "c1", lease1.fencing_token) is CompleteResult.LEASE_EXPIRED

    # c2 回收并重新领取，执行权归属 c2
    outcome, lease2 = store.claim_next("c2")
    assert outcome is LeaseResult.EXPIRED_REACQUIRED
    assert lease2.owner == "c2" and lease2.fencing_token == lease1.fencing_token + 1

    # c1 拿着旧 token 提交 -> HELD_BY_OTHER（fencing token 失效）
    assert store.complete(lease1.task_id, "c1", lease1.fencing_token) is CompleteResult.HELD_BY_OTHER
    # c2 提交成功
    assert store.complete(lease2.task_id, "c2", lease2.fencing_token) is CompleteResult.ACCEPTED
    assert store.get(lease2.task_id).status is TaskStatus.SUCCEEDED
