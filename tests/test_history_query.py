"""结果读回与执行历史查询测试：分页、按状态/时间范围过滤。"""
from __future__ import annotations

import pytest

from tasklease import (
    ResultView,
    TaskStatus,
    TaskStore,
)

from ._logutil import log_events, log_task


def _finish_one(store: TaskStore, key: str, clock, result=None) -> str:
    t, _ = store.enqueue(key)
    g = store.acquire_grant("w", lease_seconds=1000)
    assert g is not None and g.id == t.id
    store.complete(t.id, "w", result or {"key": key}, token=g.token)
    clock.advance(10)
    return t.id


def _fail_one(store: TaskStore, key: str, clock, error: str) -> str:
    t, _ = store.enqueue(key, max_attempts=1)
    g = store.acquire_grant("w", lease_seconds=1000)
    assert g is not None and g.id == t.id
    store.fail(t.id, "w", error, token=g.token)
    clock.advance(10)
    return t.id


def test_get_result_returns_output_error_and_attempts(store, clock) -> None:
    ok_id = _finish_one(store, "hq-ok", clock, {"url": "s3://out/1"})
    fail_id = _fail_one(store, "hq-fail", clock, "download failed")

    ok = store.get_result(ok_id)
    assert isinstance(ok, ResultView)
    assert ok.status is TaskStatus.SUCCEEDED
    assert ok.result == {"url": "s3://out/1"}
    assert ok.last_error is None
    assert ok.attempt == 1 and ok.max_attempts == 3
    assert ok.archived is False

    bad = store.get_result(fail_id)
    assert bad.status is TaskStatus.FAILED
    assert bad.result == {}
    assert bad.last_error == "download failed"
    log_task("结果读回", store.get(ok_id))

    with pytest.raises(Exception):
        store.get_result("missing")


def test_list_events_records_full_attempt_journey(store, clock) -> None:
    t, _ = store.enqueue("hq-journey", max_attempts=3)
    g1 = store.acquire_grant("w", lease_seconds=100)
    assert g1 is not None
    store.fail(t.id, "w", "boom-1", backoff_seconds=0, token=g1.token)
    clock.advance(1)
    g2 = store.acquire_grant("w", lease_seconds=100)
    assert g2 is not None and g2.token != g1.token
    store.complete(t.id, "w", {"ok": 1}, token=g2.token)

    page = store.list_events(task_id=t.id, limit=50)
    names = [e.event for e in page.items]
    assert names == ["enqueue.created", "acquire", "fail", "acquire", "complete"]
    # 每次领取的事件都带当次凭证指纹，可追溯租约轮换
    acquires = [e for e in page.items if e.event == "acquire"]
    assert {e.lease_token for e in acquires} == {g1.token, g2.token}
    # from/to、actor、version 都可读
    fail_ev = [e for e in page.items if e.event == "fail"][0]
    assert fail_ev.from_status == "running" and fail_ev.to_status == "pending"
    assert fail_ev.actor == "w"
    log_events(store._conn, t.id)  # type: ignore[attr-defined]


def test_list_events_pagination_no_overlap_no_loss(store, clock) -> None:
    for i in range(7):
        _finish_one(store, f"hq-page-{i}", clock)
    seen: set[int] = set()
    cursor = None
    pages = 0
    total = 0
    while True:
        page = store.list_events(limit=3, cursor=cursor)
        pages += 1
        ids = [e.id for e in page.items]
        assert len(ids) == len(set(ids)), "页内重复"
        assert not (set(ids) & seen), "跨页重复"
        seen.update(ids)
        total += len(ids)
        cursor = page.next_cursor
        if cursor is None:
            break
    assert pages >= 3
    # 7 个任务各产生 enqueue/acquire/complete 共 21 条
    assert total == 21
    assert len(seen) == 21

    # 非法 cursor 抛 ValueError（HTTP 层映射 400）
    with pytest.raises(ValueError):
        store.list_events(cursor="not-a-cursor")


def test_list_events_filters_by_status_and_time(store, clock) -> None:
    t_ok = _finish_one(store, "hq-f-ok", clock)
    t_fail = _fail_one(store, "hq-f-fail", clock, "x")

    succ = store.list_events(statuses=[TaskStatus.SUCCEEDED], limit=100)
    assert all(e.to_status == "succeeded" for e in succ.items)
    assert {e.task_id for e in succ.items} == {t_ok}

    failed = store.list_events(statuses=["failed"], limit=100)
    assert all(e.to_status == "failed" for e in failed.items)
    assert {e.task_id for e in failed.items} == {t_fail}

    # 时间范围：只看最近（失败任务更晚）
    late = store.list_events(since=clock.now - 5, limit=100)
    assert all(e.created_at >= clock.now - 5 for e in late.items)
    assert all(e.task_id == t_fail for e in late.items)
    early = store.list_events(until=clock.now - 100, limit=100)
    assert all(e.created_at <= clock.now - 100 for e in early.items)

    # actor 过滤
    ws = store.list_events(actor="w", limit=200)
    assert all(e.actor == "w" for e in ws.items)


def test_list_tasks_filter_and_pagination(store, clock) -> None:
    ids_ok = {_finish_one(store, f"hq-t-{i}", clock) for i in range(3)}
    id_fail = _fail_one(store, "hq-t-fail", clock, "e")

    succ = store.list_tasks(statuses=[TaskStatus.SUCCEEDED], limit=50)
    assert {t.id for t in succ.items} == ids_ok
    assert all(t.status is TaskStatus.SUCCEEDED for t in succ.items)

    fail_page = store.list_tasks(statuses=["failed"])
    assert {t.id for t in fail_page.items} == {id_fail}

    # 时间范围 + 分页
    page = store.list_tasks(since=clock.now - 100, limit=2)
    assert len(page.items) == 2 and page.next_cursor
    all_ids: set[str] = set()
    cursor = None
    while True:
        p = store.list_tasks(limit=2, cursor=cursor, since=0)
        all_ids.update(t.id for t in p.items)
        cursor = p.next_cursor
        if cursor is None:
            break
    assert ids_ok <= all_ids and id_fail in all_ids

    with pytest.raises(ValueError):
        store.list_tasks(limit=0)


def test_recover_events_visible_in_history(store) -> None:
    t, _ = store.enqueue("hq-recover-evt")
    g = store.acquire_grant("w", lease_seconds=10)
    assert g is not None
    store._conn.execute(  # type: ignore[attr-defined]
        "UPDATE tasks SET lease_expires_at=NULL WHERE id=?", (t.id,)
    )
    store._conn.commit()  # type: ignore[attr-defined]
    store.recover_running(task_ids=[t.id])
    ev = store.list_events(task_id=t.id, limit=50)
    assert any(e.event == "recover.requeue" for e in ev.items)
    rec = [e for e in ev.items if e.event == "recover.requeue"][0]
    assert rec.actor == "ops-recovery"
    assert rec.from_status == "running" and rec.to_status == "pending"
