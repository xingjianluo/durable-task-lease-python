"""结果/执行历史读回、分页筛选、终态归档语义不回归。"""
from __future__ import annotations

import pytest

from tasklease import (
    RejectReason,
    TaskRejected,
    TaskStatus,
    TaskStore,
)

from .conftest import claim


def test_result_and_history_readable_after_success(store) -> None:
    t, _ = store.enqueue("ok", {"in": 1}, max_attempts=2)
    claimed, tok = claim(store, "w", 100)
    done = store.complete(claimed.id, "w", tok, {"out": 42})

    result = store.get_result(t.id)
    assert result["status"] == "succeeded"
    assert result["result"] == {"out": 42}
    assert result["last_error"] is None
    assert result["attempt"] == 1
    assert result["archived"] is False

    events = store.list_events(t.id)
    names = [e.event for e in events]
    assert names == ["enqueue.created", "acquire", "complete"]
    # 归属变化可见：complete 的 actor 与 token 都记录在案
    complete_ev = [e for e in events if e.event == "complete"][0]
    assert complete_ev.actor == "w" and complete_ev.lease_token == tok
    assert complete_ev.from_status == "running"
    assert complete_ev.to_status == "succeeded"


def test_failure_history_records_errors_and_attempts(store, clock) -> None:
    t, _ = store.enqueue("flaky", max_attempts=2)
    c1, tok1 = claim(store, "w", 100)
    store.fail(t.id, "w", tok1, "boom-1", backoff_seconds=0)
    c2, tok2 = claim(store, "w", 100)
    store.fail(t.id, "w", tok2, "boom-2")  # 耗尽 -> FAILED

    result = store.get_result(t.id)
    assert result["status"] == "failed"
    assert result["result"] is None
    assert result["last_error"] == "boom-2"
    assert result["attempt"] == 2

    events = store.list_events(t.id)
    fails = [e for e in events if e.event == "fail"]
    assert [e.reason for e in fails if "boom-1" in (e.reason or "")]
    assert [e for e in fails if "boom-2" in (e.reason or "")]
    assert {e.lease_token for e in fails} == {tok1, tok2}
    assert [e.to_status for e in fails] == ["pending", "failed"]


def test_list_tasks_filter_and_pagination(store, clock) -> None:
    # 3 pending + 2 succeeded：交错入队/领取/完成，避免队列顺序干扰
    for i in range(2):
        t, _ = store.enqueue(f"s{i}")
        claimed = store.acquire("w", lease_seconds=100)
        assert claimed is not None
        c, tok = claimed
        assert c.id == t.id
        store.complete(c.id, "w", tok)
    for i in range(3):
        store.enqueue(f"p{i}")

    only_pending = store.list_tasks(statuses=[TaskStatus.PENDING], limit=10)
    assert {t.idempotency_key for t in only_pending.items} == {"p0", "p1", "p2"}
    only_done = store.list_tasks(
        statuses=[TaskStatus.SUCCEEDED, TaskStatus.FAILED], limit=10
    )
    assert len(only_done.items) == 2

    # 分页：limit=2 稳定翻页，游标不重不漏
    seen: set[str] = set()
    cursor = None
    pages = 0
    while True:
        page = store.list_tasks(limit=2, cursor=cursor)
        seen |= {t.id for t in page.items}
        pages += 1
        if page.next_cursor is None:
            break
        cursor = page.next_cursor
    assert len(seen) == 5 and pages == 3

    # 时间范围筛选
    now = clock.now
    future = store.list_tasks(created_from=now + 1)
    assert future.items == []
    all_of_them = store.list_tasks(created_to=now)
    assert len(all_of_them.items) == 5


def test_archive_moves_terminal_and_keeps_readback(store, clock) -> None:
    t, _ = store.enqueue("arch", max_attempts=1)
    c, tok = claim(store, "w", 100)
    store.complete(c.id, "w", tok, {"final": True})

    # 未到年龄不归档
    assert store.archive_terminal(min_age_seconds=10) == 0
    clock.advance(11)
    n = store.archive_terminal(min_age_seconds=10, batch_size=10)
    assert n == 1

    # 主表已删除，归档表可读：最终状态 + 产出
    assert store._fetch_by_id(t.id) is None  # type: ignore[attr-defined]
    got = store.get(t.id)
    assert got.status is TaskStatus.SUCCEEDED and got.result == {"final": True}
    result = store.get_result(t.id)
    assert result["archived"] is True and result["result"] == {"final": True}

    # 执行历史同样可读，且包含归档事件
    events = store.list_events(t.id)
    assert any(e.event == "complete" for e in events)
    archive_ev = [e for e in events if e.event == "archive"]
    assert archive_ev and archive_ev[0].archived is True


def test_archive_preserves_idempotency_conclusion(store, clock) -> None:
    t, _ = store.enqueue("once", max_attempts=1)
    c, tok = claim(store, "w", 100)
    store.fail(c.id, "w", tok, "dead")  # FAILED 终态
    clock.advance(11)
    assert store.archive_terminal(min_age_seconds=10) == 1

    # 归档后重复入队：既有结论（task_terminal）不受影响
    with pytest.raises(TaskRejected) as exc:
        store.enqueue("once")
    assert exc.value.reason is RejectReason.TASK_TERMINAL
    # 幂等查询仍能找到归档任务
    assert store.get_by_idempotency_key("once").status is TaskStatus.FAILED


def test_archive_excludes_live_and_recent(store, clock) -> None:
    # 足够老的终态：先终结，再把时钟推老
    o, _ = store.enqueue("o", max_attempts=1)
    c2, tok2 = claim(store, "w2", 100)
    assert c2.id == o.id
    store.complete(c2.id, "w2", tok2, {"old": True})
    clock.advance(100)
    # 刚终结的不归档（在老任务终结之后才完成）
    r, _ = store.enqueue("r", max_attempts=1)
    c, tok = claim(store, "w", 100)
    assert c.id == r.id
    store.complete(c.id, "w", tok)
    # 非终态不归档（排在领取队列最后，不会被前面的 claim 误领）
    p, _ = store.enqueue("p")

    n = store.archive_terminal(min_age_seconds=10)
    assert n == 1
    assert store.get(p.id).status is TaskStatus.PENDING
    assert store.get(r.id).status is TaskStatus.SUCCEEDED  # 仍在主表
    assert store.get(o.id).archived_at is not None


def test_archive_dry_run_counts_without_writing(store, clock) -> None:
    for i in range(3):
        t, _ = store.enqueue(f"d{i}", max_attempts=1)
        c, tok = claim(store, "w", 100)
        store.complete(c.id, "w", tok)
    clock.advance(100)
    planned = store.archive_terminal(min_age_seconds=10, dry_run=True)
    assert planned == 3
    # 主表无变化
    page = store.list_tasks(statuses=[TaskStatus.SUCCEEDED])
    assert len(page.items) == 3


def test_archive_batched_and_does_not_block_new_work(store, clock) -> None:
    # 制造 5 个老终态
    for i in range(5):
        t, _ = store.enqueue(f"old{i}", max_attempts=1)
        c, tok = claim(store, "w", 100)
        store.complete(c.id, "w", tok)
    clock.advance(100)
    assert store.archive_terminal(min_age_seconds=10, batch_size=2) == 5

    # 归档后新任务照常领取与提交
    fresh, _ = store.enqueue("fresh")
    c, tok = claim(store, "new-worker", 100)
    assert c.idempotency_key == "fresh"
    done = store.complete(c.id, "new-worker", tok, {"v": 1})
    assert done.status is TaskStatus.SUCCEEDED

    # 归档表筛选
    archived = store.list_archived_tasks(statuses=[TaskStatus.SUCCEEDED], limit=10)
    assert len(archived.items) == 5
    # 新完成的任务不在归档表
    assert all(t.id != fresh.id for t in archived.items)


def test_archive_is_idempotent(store, clock) -> None:
    t, _ = store.enqueue("once2", max_attempts=1)
    c, tok = claim(store, "w", 100)
    store.complete(c.id, "w", tok)
    clock.advance(50)
    assert store.archive_terminal(min_age_seconds=10) == 1
    # 再跑没有可搬的，也不报错
    assert store.archive_terminal(min_age_seconds=10) == 0
    # 归档事件只有一条
    events = store.list_events(t.id)
    assert len([e for e in events if e.event == "archive"]) == 1
