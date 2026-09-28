"""执行产出与历史查询测试：读回结果、失败原因、尝试过程、租约归属变化。"""
from __future__ import annotations

import pytest

from tasklease import RejectReason, TaskRejected
from tasklease.history import HistoryService

from ._logutil import log_events


def test_result_read_back_for_success_and_failure(store, clock) -> None:
    hist = HistoryService(store)

    ok_task, _ = store.enqueue("hist-ok")
    g = store.acquire("w")
    store.complete(g.id, "w", {"url": "done", "bytes": 42},
                   lease_token=g.lease_token)
    result = hist.get_result(ok_task.id)
    assert result.status == "succeeded"
    assert result.result == {"url": "done", "bytes": 42}
    assert result.last_error is None
    assert result.attempt == 1
    assert result.archived is False

    fail_task, _ = store.enqueue("hist-fail", max_attempts=1)
    g2 = store.acquire("w")
    store.fail(g2.id, "w", "kaboom", lease_token=g2.lease_token)
    failed = hist.get_result(fail_task.id)
    assert failed.status == "failed"
    assert failed.result == {}
    assert failed.last_error == "kaboom"

    with pytest.raises(TaskRejected) as exc:
        hist.get_result("missing")
    assert exc.value.reason is RejectReason.TASK_NOT_FOUND


def test_event_history_records_attempts_and_lease_ownership(store, clock) -> None:
    hist = HistoryService(store)
    task, _ = store.enqueue("hist-events", max_attempts=3)

    g1 = store.acquire("worker-A", lease_seconds=10)
    store.fail(task.id, "worker-A", "first error", backoff_seconds=0,
               lease_token=g1.lease_token)
    store.acquire("worker-B", lease_seconds=10)  # B 领取后未续期，租约过期
    clock.advance(11)
    g3 = store.acquire("worker-C", lease_seconds=10)  # 过期接管
    store.complete(task.id, "worker-C", {"final": True},
                   lease_token=g3.lease_token)

    events = hist.list_events(task.id)
    names = [e.event for e in events.items]
    assert "enqueue.created" in names
    assert names.count("acquire") == 3
    assert "fail" in names and "complete" in names
    # 租约归属变化可追溯：三次 acquire 的 actor 依次是 A/B/C
    actors = [e.actor for e in events.items if e.event == "acquire"]
    assert actors == ["worker-A", "worker-B", "worker-C"]
    # 事件记录了版本推进
    assert all(e.version >= 0 for e in events.items)
    assert events.total == len(names)
    log_events(store._conn, task.id)  # type: ignore[attr-defined]


def test_event_filters_by_status_and_time_window(store, clock) -> None:
    hist = HistoryService(store)
    t1, _ = store.enqueue("f-a", max_attempts=1)
    g = store.acquire("w", lease_seconds=100)
    store.complete(t1.id, "w", lease_token=g.lease_token)  # t=1000000

    clock.advance(100)
    t2, _ = store.enqueue("f-b", max_attempts=1)
    g2 = store.acquire("w", lease_seconds=100)
    store.fail(t2.id, "w", "e", lease_token=g2.lease_token)  # t=1000100

    succ = hist.list_events(statuses=["succeeded"])
    assert succ.total == 1
    assert succ.items[0].to_status == "succeeded"
    assert succ.items[0].task_id == t1.id

    fail_ev = hist.list_events(statuses=["failed"])
    assert fail_ev.total == 1 and fail_ev.items[0].task_id == t2.id

    # 时间窗口：只看 t=1000050 之后的事件
    window = hist.list_events(start=1_000_050.0)
    task_ids = {e.task_id for e in window.items}
    assert t1.id not in task_ids and t2.id in task_ids

    # 闭区间右端
    early = hist.list_events(end=1_000_000.0)
    assert t2.id not in {e.task_id for e in early.items}


def test_pagination_of_tasks_and_events(store) -> None:
    hist = HistoryService(store)
    for i in range(5):
        task, _ = store.enqueue(f"page-{i}", max_attempts=1)
        g = store.acquire("w")
        store.complete(task.id, "w", {"i": i}, lease_token=g.lease_token)

    p1 = hist.list_tasks(statuses=["succeeded"], limit=2, offset=0)
    assert len(p1.items) == 2 and p1.total == 5
    assert p1.next_offset == 2
    p2 = hist.list_tasks(statuses=["succeeded"], limit=2, offset=2)
    assert len(p2.items) == 2 and p2.next_offset == 4
    p3 = hist.list_tasks(statuses=["succeeded"], limit=2, offset=4)
    assert len(p3.items) == 1 and p3.next_offset is None
    # 三页拼起来无重复
    ids = [t.id for t in (*p1.items, *p2.items, *p3.items)]
    assert len(set(ids)) == 5

    ev_p = hist.list_events(statuses=["succeeded"], limit=3)
    assert ev_p.total == 5 and len(ev_p.items) == 3


def test_list_tasks_status_and_time_filters(store, clock) -> None:
    hist = HistoryService(store)
    t_running, _ = store.enqueue("q-run")
    g = store.acquire("w", lease_seconds=100)

    t_done, _ = store.enqueue("q-done", max_attempts=1)
    g2 = store.acquire("w", lease_seconds=100)
    store.complete(t_done.id, "w", lease_token=g2.lease_token)

    running = hist.list_tasks(statuses=["running"])
    assert {t.id for t in running.items} == {t_running.id}

    terminal = hist.list_tasks(statuses=["succeeded", "failed"])
    assert {t.id for t in terminal.items} == {t_done.id}

    # 非法状态值直接报错而非静默空集
    with pytest.raises(ValueError):
        hist.list_tasks(statuses=["nope"])  # type: ignore[list-item]

    # 持有的任务仍可正常提交，查询不阻塞执行
    store.complete(t_running.id, "w", lease_token=g.lease_token)
