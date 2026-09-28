"""终态任务归档测试：归档范围、透明读回、幂等入队结论不回归、不阻塞执行。"""
from __future__ import annotations

import pytest

from tasklease import (
    ArchiveOutcome,
    ArchiveService,
    EnqueueOutcome,
    RejectReason,
    TaskRejected,
    TaskStatus,
    TaskStore,
)
from tasklease.history import HistoryService

from ._logutil import log_task


def _finish_ok(st: TaskStore, key: str, result=None) -> str:
    task, _ = st.enqueue(key, max_attempts=1)
    g = st.acquire("w", lease_seconds=1000)
    st.complete(task.id, "w", result or {"k": key}, lease_token=g.lease_token)
    return task.id


def test_only_terminal_archived_and_age_window(store, clock) -> None:
    arc = ArchiveService(store)
    old_id = _finish_ok(store, "arc-old")
    clock.advance(100)
    new_id = _finish_ok(store, "arc-new")
    # 还有一个运行中的任务，任何时候都不能被归档
    run_task, _ = store.enqueue("arc-running")
    run_g = store.acquire("w", lease_seconds=1000)

    # 年龄窗口 50：只归档 100 秒前终结的 old
    report = arc.archive_terminal(older_than_seconds=50, limit=100)
    assert report.archived == 1
    assert store.get(old_id).archived is True
    assert store.get(new_id).archived is False
    assert store.get(run_task.id).status is TaskStatus.RUNNING

    # 单归档非终态 -> skipped
    assert arc.archive_one(run_task.id) is ArchiveOutcome.SKIPPED_NON_TERMINAL
    # 运行任务凭证仍然有效，归档过程没有影响执行
    done = store.complete(run_task.id, "w", lease_token=run_g.lease_token)
    assert done.status is TaskStatus.SUCCEEDED

    assert arc.archive_one(new_id) is ArchiveOutcome.ARCHIVED
    assert arc.archive_one("missing") is ArchiveOutcome.NOT_FOUND
    log_task("已归档任务读回", store.get(old_id))


def test_archived_result_and_status_still_readable(store) -> None:
    arc = ArchiveService(store)
    hist = HistoryService(store)
    ok_id = _finish_ok(store, "arc-read-ok", {"output": "x"})

    fail_task, _ = store.enqueue("arc-read-fail", max_attempts=1)
    g = store.acquire("w")
    store.fail(fail_task.id, "w", "terminal-boom", lease_token=g.lease_token)
    arc.archive_terminal()

    # get 透明读回，状态与产出完整
    ok = store.get(ok_id)
    assert ok.status is TaskStatus.SUCCEEDED and ok.archived is True
    result = hist.get_result(ok_id)
    assert result.result == {"output": "x"} and result.archived is True

    failed = hist.get_result(fail_task.id)
    assert failed.status == "failed" and failed.last_error == "terminal-boom"

    # 历史事件不随归档删除：尝试过程仍可查
    events = hist.list_events(fail_task.id)
    assert any(e.event == "fail" for e in events.items)

    # include_archived 时归档任务出现在列表里并带标记
    page = hist.list_tasks(statuses=["succeeded", "failed"], include_archived=True)
    ids = {t.id: t.archived for t in page.items}
    assert ids.get(ok_id) is True
    # 默认不含归档
    page_live = hist.list_tasks(statuses=["succeeded", "failed"])
    assert all(not t.archived for t in page_live.items)
    assert page_live.total == 0


def test_idempotent_enqueue_conclusion_unchanged_after_archive(store, clock) -> None:
    # 归档不影响“非终态 RUNNING 过期行重复入队收敛 EXISTING”的既有结论。
    task, _ = store.enqueue("arc-key-run")
    store.acquire("w", lease_seconds=10)
    clock.advance(11)
    same, outcome = store.enqueue("arc-key-run")
    assert same.id == task.id and outcome is EnqueueOutcome.EXISTING
    # 排空这个过期运行任务，避免干扰后续终态流程
    reclaim = store.acquire("w", lease_seconds=10)
    assert reclaim is not None and reclaim.id == task.id
    store.complete(task.id, "w", lease_token=reclaim.lease_token)

    arc = ArchiveService(store)
    task_id = _finish_ok(store, "arc-key")
    arc.archive_terminal()

    # 归档后同 key 重复入队：仍然按终态拒绝（结论不回归）
    with pytest.raises(TaskRejected) as exc:
        store.enqueue("arc-key")
    assert exc.value.reason is RejectReason.TASK_TERMINAL
    assert store.get(task_id).archived is True


def test_archive_is_idempotent(store, clock) -> None:
    arc = ArchiveService(store)
    tid = _finish_ok(store, "arc-idem")
    r1 = arc.archive_terminal()
    assert r1.archived == 1
    # 再跑：主表已无候选，零新增、零副作用
    r2 = arc.archive_terminal()
    assert r2.archived == 0 and r2.actions == []
    # 单条重复归档：already_archived
    assert arc.archive_one(tid) is ArchiveOutcome.ALREADY_ARCHIVED
    # 只还能读到一份（主表与归档表不重复）
    hist = HistoryService(store)
    page = hist.list_tasks(include_archived=True)
    assert [t.id for t in page.items].count(tid) == 1


def test_archive_does_not_block_new_claim_and_complete(store, clock) -> None:
    arc = ArchiveService(store)
    for i in range(6):
        _finish_ok(store, f"arc-bulk-{i}")
    # 归档与新任务交错：归档跑起来的同时新任务正常领取提交
    new_task, _ = store.enqueue("arc-live-job")
    report = arc.archive_terminal(limit=3)
    assert report.archived == 3  # limit 生效
    g = store.acquire("live-worker")
    assert g is not None and g.id == new_task.id
    done = store.complete(new_task.id, "live-worker", {"live": True},
                          lease_token=g.lease_token)
    assert done.status is TaskStatus.SUCCEEDED
    # 剩余终态可继续归档
    rest = arc.archive_terminal()
    assert rest.archived == 4  # 3(首批外剩余) + live-job 已完成
