"""服务门面测试：重点断言日志中的入队标识、租约信息与状态流转判定依据。"""

import logging

import pytest

from task_store.clock import FakeClock
from task_store.service import DurableTaskService
from task_store.types import (
    CompleteResult,
    LeaseResult,
    ReleaseResult,
    RenewResult,
    RejectReason,
    RetryResult,
    StateTransitionError,
    TaskStatus,
)


@pytest.fixture()
def svc(tmp_path, caplog):
    caplog.set_level(logging.INFO, logger="task_store.service")
    s = DurableTaskService(
        str(tmp_path / "svc.db"), FakeClock(1000.0),
        lease_seconds=10, default_max_attempts=1,
        backoff_base_seconds=5, backoff_factor=2,
    )
    yield s
    s.close()


def test_logs_enqueue_lease_and_transition(svc, caplog):
    svc.enqueue("order-42", "do-payment")
    assert "入队标识=order-42" in caplog.text
    assert "判定=CREATED" in caplog.text

    # 重复入队（PENDING）
    svc.enqueue("order-42")
    assert "判定=DEDUPED_PENDING" in caplog.text

    outcome, lease = svc.claim("worker-A")
    assert outcome is LeaseResult.ACQUIRED
    assert "owner=worker-A" in caplog.text
    assert "fencing=" in caplog.text and "剩余=" in caplog.text

    svc.renew(lease.task_id, "worker-A", lease.fencing_token, lease_seconds=30)
    assert "判定=RENEWED" in caplog.text

    verdict = svc.complete(lease.task_id, "worker-A", lease.fencing_token, "OK")
    assert verdict is CompleteResult.ACCEPTED
    assert "判定=ACCEPTED" in caplog.text
    assert "当前状态=SUCCEEDED" in caplog.text
    assert svc.get_task(lease.task_id).status is TaskStatus.SUCCEEDED


def test_logs_reject_reasons_distinguishable(svc, caplog):
    svc.enqueue("k-1")
    _, lease = svc.claim("w1")

    # 其他持有者
    assert svc.renew(lease.task_id, "wX", lease.fencing_token)[0] is RenewResult.HELD_BY_OTHER
    assert "判定=HELD_BY_OTHER" in caplog.text

    # 过期后三类操作
    svc._clock.advance(10.1)
    assert svc.renew(lease.task_id, "w1", lease.fencing_token)[0] is RenewResult.LEASE_EXPIRED
    assert svc.release(lease.task_id, "w1", lease.fencing_token) is ReleaseResult.LEASE_EXPIRED
    assert "判定=LEASE_EXPIRED" in caplog.text
    assert svc.complete(lease.task_id, "w1", lease.fencing_token) is CompleteResult.LEASE_EXPIRED

    # 回收重领 + 重试耗尽（max_attempts=1）
    outcome, lease2 = svc.claim("w2")
    assert outcome is LeaseResult.EXPIRED_REACQUIRED
    assert "判定=EXPIRED_REACQUIRED" in caplog.text
    verdict, task = svc.fail(lease2.task_id, "w2", lease2.fencing_token, "network down")
    assert verdict is RetryResult.EXHAUSTED
    assert task.status is TaskStatus.FAILED
    assert "判定=EXHAUSTED" in caplog.text
    # 回收重新领取本身计为一次新尝试：共领取 2 次，超过 max_attempts=1
    assert "attempt=2/1" in caplog.text

    with pytest.raises(StateTransitionError) as ei:
        svc.enqueue("k-1")
    assert ei.value.reason is RejectReason.ALREADY_TERMINAL
