"""执行凭证（lease_token）专项：轮换即失效、旧执行不得覆盖新产出。"""
from __future__ import annotations

import pytest

from tasklease import (
    HeartbeatOutcome,
    ReleaseOutcome,
    RejectReason,
    TaskRejected,
    TaskStatus,
    TaskStore,
)

from ._logutil import log_events, log_task
from .conftest import claim


def test_token_rotates_on_every_acquire(store) -> None:
    task, _ = store.enqueue("rot")
    t1, tok1 = claim(store, "c1")
    assert tok1 and t1.lease_token == tok1
    assert store.release(task.id, "c1", tok1) is ReleaseOutcome.RELEASED
    t2, tok2 = claim(store, "c2")
    assert tok1 != tok2 and t2.lease_token == tok2
    store.fail(task.id, "c2", tok2, "retry-me", backoff_seconds=0)
    t3, tok3 = claim(store, "c3")
    assert len({tok1, tok2, tok3}) == 3


def test_old_token_rejected_from_rotation_instant(store, clock) -> None:
    """租约轮换的那一刻，旧凭证立刻失效——即便新租约尚未过期。"""
    task, _ = store.enqueue("instant")
    t1, tok1 = claim(store, "same", lease_seconds=100)
    clock.advance(101)
    t2, tok2 = claim(store, "same", lease_seconds=100)
    assert t2.id == t1.id

    # 旧凭证的所有写操作一律被拒，且任务产出未被改动
    with pytest.raises(TaskRejected) as e:
        store.complete(task.id, "same", tok1, {"hijacked": True})
    assert e.value.reason is RejectReason.INVALID_TOKEN
    with pytest.raises(TaskRejected) as e:
        store.fail(task.id, "same", tok1, "boom")
    assert e.value.reason is RejectReason.INVALID_TOKEN
    assert store.heartbeat(task.id, "same", tok1, 100) is HeartbeatOutcome.INVALID_TOKEN
    assert store.release(task.id, "same", tok1) is ReleaseOutcome.INVALID_TOKEN

    done = store.complete(task.id, "same", tok2, {"legit": True})
    assert done.result == {"legit": True}
    log_events(store._conn, task.id)  # type: ignore[attr-defined]


def test_old_cannot_overwrite_new_then_new_not_duplicate(store, clock) -> None:
    """旧执行先成功、新执行后完成的覆盖场景：旧执行必须被挡，新执行不被当重复。"""
    task, _ = store.enqueue("cover")
    t1, tok1 = claim(store, "w", lease_seconds=10)
    clock.advance(11)
    t2, tok2 = claim(store, "w", lease_seconds=10)

    # 旧执行迟到的“成功”被拒
    with pytest.raises(TaskRejected) as e:
        store.complete(task.id, "w", tok1, {"v": "old"})
    assert e.value.reason is RejectReason.INVALID_TOKEN
    assert store.get(task.id).status is TaskStatus.RUNNING

    # 新执行完成成功，没有被误判成重复提交
    done = store.complete(task.id, "w", tok2, {"v": "new"})
    assert done.status is TaskStatus.SUCCEEDED
    assert store.get_result(task.id)["result"] == {"v": "new"}
    log_task("新执行产出保留", done)


def test_distinct_reasons_expired_vs_taken_vs_bad_token(store, clock) -> None:
    """三类结论必须分开：租约过期 / 已被别人接管 / 凭证对不上。"""
    # 1) 租约过期、尚未被接管 -> lease_expired
    t, _ = store.enqueue("r1")
    t1, tok1 = claim(store, "c1", lease_seconds=10)
    clock.advance(11)
    with pytest.raises(TaskRejected) as e:
        store.complete(t.id, "c1", tok1)
    assert e.value.reason is RejectReason.LEASE_EXPIRED

    # 2) 已被别人接管（不同消费者名） -> lease_held_by_other
    t2, tok2 = claim(store, "c2", lease_seconds=10)
    with pytest.raises(TaskRejected) as e:
        store.complete(t.id, "c1", tok1)
    assert e.value.reason is RejectReason.LEASE_HELD_BY_OTHER

    # 3) 同消费者名、凭证对不上 -> invalid_token
    clock.advance(11)
    t3, tok3 = claim(store, "c1", lease_seconds=10)
    with pytest.raises(TaskRejected) as e:
        store.complete(t.id, "c1", tok1)
    assert e.value.reason is RejectReason.INVALID_TOKEN
    # 当前有效凭证依然可用
    assert store.complete(t.id, "c1", tok3).status is TaskStatus.SUCCEEDED
    log_events(store._conn, t.id)  # type: ignore[attr-defined]


def test_forged_token_never_accepted(store) -> None:
    t, _ = store.enqueue("forge")
    claim(store, "c1")
    with pytest.raises(TaskRejected) as e:
        store.complete(t.id, "c1", "a" * 32)
    assert e.value.reason is RejectReason.INVALID_TOKEN


def test_events_carry_token_for_ownership_audit(store, clock) -> None:
    """审计历史能看出每次执行归属了哪个凭证。"""
    t, _ = store.enqueue("audit")
    t1, tok1 = claim(store, "w", 10)
    assert store.heartbeat(t.id, "w", tok1, 20) is HeartbeatOutcome.RENEWED
    clock.advance(21)
    t2, tok2 = claim(store, "w", 10)
    store.complete(t.id, "w", tok2, {"ok": 1})

    events = store.list_events(t.id)
    by_event = {}
    for ev in events:
        by_event.setdefault(ev.event, []).append(ev.lease_token)
    assert by_event["acquire"] == [tok1, tok2]
    assert by_event["heartbeat"] == [tok1]
    assert by_event["complete"] == [tok2]
    # 能按凭证串起每次尝试的完整轨迹
    first_run = [e for e in events if e.lease_token == tok1]
    assert {e.event for e in first_run} == {"acquire", "heartbeat"}
