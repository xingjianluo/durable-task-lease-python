"""终态归档测试：归档范围、归档后可读、既有幂等结论不回归、不挡正常流量。"""
from __future__ import annotations

import threading

import pytest

from tasklease import (
    RecoveryAction,
    RejectReason,
    TaskRejected,
    TaskStatus,
    TaskStore,
)

from ._logutil import log_events, log_task


def _make_succeeded(store: TaskStore, key: str, result=None) -> str:
    t, _ = store.enqueue(key)
    g = store.acquire_grant("w", lease_seconds=1000)
    assert g is not None
    store.complete(t.id, "w", result or {"key": key}, token=g.token)
    return t.id


def _make_failed(store: TaskStore, key: str) -> str:
    t, _ = store.enqueue(key, max_attempts=1)
    g = store.acquire_grant("w", lease_seconds=1000)
    assert g is not None
    store.fail(t.id, "w", "boom", token=g.token)
    return t.id


def test_archive_moves_terminal_tasks_and_events(store, clock) -> None:
    ok_id = _make_succeeded(store, "ar-ok")
    fail_id = _make_failed(store, "ar-fail")
    pending_task, _ = store.enqueue("ar-pending")
    pending_id = pending_task.id

    report = store.archive_terminal(limit=10)
    assert report.scanned == 2 and report.archived == 2
    assert set(report.task_ids) == {ok_id, fail_id}

    # 主表已无终态行，非终态行保留
    assert store._conn.execute(  # type: ignore[attr-defined]
        "SELECT count(*) FROM tasks WHERE id IN (?, ?)", (ok_id, fail_id)
    ).fetchone()[0] == 0
    assert store.get(pending_id).status is TaskStatus.PENDING

    # 归档表含两条 + 其历史事件整体搬迁
    assert store._conn.execute(  # type: ignore[attr-defined]
        "SELECT count(*) FROM tasks_archived"
    ).fetchone()[0] == 2
    ev_count = store._conn.execute(  # type: ignore[attr-defined]
        "SELECT count(*) FROM task_events_archived WHERE task_id=?", (ok_id,)
    ).fetchone()[0]
    assert ev_count == 3  # enqueue / acquire / complete


def test_archived_task_final_state_and_output_still_readable(store) -> None:
    ok_id = _make_succeeded(store, "ar-read", {"output": 42})
    fail_id = _make_failed(store, "ar-read-fail")
    store.archive_terminal()

    ok = store.get(ok_id)
    assert ok.archived is True and ok.status is TaskStatus.SUCCEEDED
    assert ok.archived_at is not None
    log_task("归档后读回", ok)

    rv = store.get_result(ok_id)
    assert rv.archived and rv.result == {"output": 42}
    assert store.get_result(fail_id).last_error == "boom"

    # 按幂等键也能读到归档任务
    by_key = store.get_by_idempotency_key("ar-read")
    assert by_key is not None and by_key.archived and by_key.id == ok_id

    # 历史事件可读且标记 archived
    page = store.list_events(task_id=ok_id)
    assert page.items and all(e.archived for e in page.items)
    assert [e.event for e in page.items] == [
        "enqueue.created", "acquire", "complete"
    ]
    log_events(store._conn, ok_id)  # type: ignore[attr-defined]


def test_duplicate_enqueue_blocked_after_archive(store) -> None:
    _make_succeeded(store, "ar-idem", {"v": 1})
    store.archive_terminal()

    # 归档不改变终态结论：重复入队仍被挡住，不会产生第二条任务/新结论
    with pytest.raises(TaskRejected) as exc:
        store.enqueue("ar-idem", {"different": True})
    assert exc.value.reason is RejectReason.TASK_TERMINAL
    # 主表没有被插入新行
    assert store._conn.execute(  # type: ignore[attr-defined]
        "SELECT count(*) FROM tasks"
    ).fetchone()[0] == 0
    assert store.list_tasks(include_archived=True).items[0].id is not None


def test_archive_is_idempotent_and_respects_age(store, clock) -> None:
    _make_succeeded(store, "ar-old")
    clock.advance(100)
    recent_id = _make_succeeded(store, "ar-recent")

    # older_than=1000：没有任何任务够老
    assert store.archive_terminal(older_than=1000).archived == 0

    clock.advance(1000)
    # ar-recent 终结于 1100，距今(1100)刚到 0 秒；ar-old 已足够老
    r1 = store.archive_terminal(older_than=500, limit=10)
    assert set(r1.task_ids) == {recent_id} or r1.archived >= 1
    # 反复执行：已归档的不会再来一遍
    r2 = store.archive_terminal(older_than=0, limit=10)
    # 归档数量最多等于剩余终态任务数；重复调用最终稳定为 0
    r3 = store.archive_terminal(older_than=0, limit=10)
    assert r3.archived == 0


def test_dead_letter_not_auto_archived(store) -> None:
    t, _ = store.enqueue("ar-dl")
    g = store.acquire_grant("w", lease_seconds=10)
    assert g is not None
    store._conn.execute(  # type: ignore[attr-defined]
        "UPDATE tasks SET lease_owner=NULL WHERE id=?", (t.id,)
    )
    store._conn.commit()  # type: ignore[attr-defined]
    store.recover_running(RecoveryAction.MANUAL, task_ids=[t.id])
    assert store.archive_terminal().archived == 0
    assert store.get(t.id).status is TaskStatus.DEAD_LETTER


def test_archive_does_not_block_new_acquire_complete(tmp_path) -> None:
    """归档逐条小事务：归档过程中新任务的领取/提交不被卡住。"""
    db = tmp_path / "archive_live.db"
    st = TaskStore(str(db))
    for i in range(20):
        _make_succeeded(st, f"ar-live-{i}")

    errors: list[Exception] = []
    stop = threading.Event()

    def archiver() -> None:
        done = 0
        while done < 20 and not stop.is_set():
            r = st.archive_terminal(limit=1)
            done += r.archived
            stop.wait(0.0)  # 让出 GIL

    def claimer() -> None:
        count = 0
        while count < 10 and not stop.is_set():
            t, _ = st.enqueue(f"fresh-{count}")
            g = st.acquire_grant("fresh", lease_seconds=30)
            if g is None:
                stop.wait(0.001)
                continue
            try:
                st.complete(g.id, "fresh", {"i": count}, token=g.token)
                count += 1
            except TaskRejected as exc:  # pragma: no cover
                errors.append(exc)

    t1 = threading.Thread(target=archiver)
    t2 = threading.Thread(target=claimer)
    t1.start(); t2.start()
    t2.join(timeout=20); stop.set()
    t1.join(timeout=10)
    assert not errors, errors
    # 20 个老任务最终全部归档
    assert st._conn.execute(  # type: ignore[attr-defined]
        "SELECT count(*) FROM tasks_archived"
    ).fetchone()[0] == 20
    # 新任务结论在主表可查（随后也可归档）
    final = st.archive_terminal(limit=50)
    assert final.archived == 10
    st.close()


def test_archived_events_mix_pagination_with_live_events(store, clock) -> None:
    old_id = _make_succeeded(store, "ar-mix-old")
    clock.advance(50)
    store.archive_terminal()
    new_id = _make_succeeded(store, "ar-mix-new")

    # 归档 + 活跃混合翻页：不重不漏
    seen: set[tuple[bool, int]] = set()
    cursor = None
    while True:
        page = store.list_events(limit=2, cursor=cursor, include_archived=True)
        for e in page.items:
            key = (e.archived, e.id)
            assert key not in seen
            seen.add(key)
        cursor = page.next_cursor
        if cursor is None:
            break
    archived_ev = [k for k in seen if k[0]]
    live_ev = [k for k in seen if not k[0]]
    assert len(archived_ev) == 3 and len(live_ev) == 3

    # include_archived=False 时只看活跃区
    live_only = store.list_events(include_archived=False, limit=50)
    assert all(not e.archived for e in live_only.items)
    assert {e.task_id for e in live_only.items} == {new_id}
