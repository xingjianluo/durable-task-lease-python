"""旧版本升级遗留的异常 RUNNING 任务：三分类、恢复、幂等且不误伤。"""
from __future__ import annotations

import sqlite3

import pytest

from tasklease import (
    RecoverOutcome,
    RejectReason,
    RunningLeaseState,
    TaskRejected,
    TaskStatus,
    TaskStore,
)

from ._logutil import log_task
from .conftest import claim


def _legacy_db(tmp_path, name: str, rows):
    db = tmp_path / name
    raw = sqlite3.connect(db)
    raw.execute(
        "CREATE TABLE tasks (id TEXT PRIMARY KEY, status TEXT NOT NULL, "
        "idempotency_key TEXT NOT NULL DEFAULT '')"
    )
    raw.executemany(
        "INSERT INTO tasks (id,status,idempotency_key) VALUES (?,?,?)", rows
    )
    raw.commit()
    raw.close()
    return str(db)


def test_three_way_classification(store, clock) -> None:
    # active：字段完整且未过期
    a, _ = store.enqueue("a")
    ta, toka = claim(store, "w", lease_seconds=100)
    assert store.classify_running(a.id) is RunningLeaseState.ACTIVE

    # expired：字段完整但过期
    clock.advance(101)
    assert store.classify_running(a.id) is RunningLeaseState.EXPIRED

    # corrupt：缺字段（旧版本遗留），直接在存储里插一条缺租约列的 running 行
    with store._conn:  # type: ignore[attr-defined]
        store._conn.execute(  # type: ignore[attr-defined]
            "INSERT INTO tasks (id,idempotency_key,payload,status,attempt,"
            "max_attempts,version,created_at,updated_at,available_at) "
            "VALUES ('c1','k-c','{}','running',0,1,0,1,1,0)"
        )
    assert store.classify_running("c1") is RunningLeaseState.CORRUPT


def test_list_stuck_excludes_active(store, clock) -> None:
    a, _ = store.enqueue("a")
    claim(store, "w", lease_seconds=100)
    with store._conn:  # type: ignore[attr-defined]
        store._conn.execute(  # type: ignore[attr-defined]
            "INSERT INTO tasks (id,idempotency_key,payload,status,attempt,"
            "max_attempts,version,created_at,updated_at,available_at) "
            "VALUES ('c1','k-c','{}','running',0,1,0,1,1,0)"
        )
    b, _ = store.enqueue("b")
    tb, tokb = claim(store, "w2", lease_seconds=10)
    clock.advance(11)  # b 过期

    stuck = {t.id: t for t in store.list_stuck_running()}
    assert set(stuck) == {"c1", b.id}
    assert a.id not in stuck  # active 不出现


def test_recover_corrupt_quarantines_and_requeues_expired(store, clock) -> None:
    with store._conn:  # type: ignore[attr-defined]
        store._conn.execute(  # type: ignore[attr-defined]
            "INSERT INTO tasks (id,idempotency_key,payload,status,attempt,"
            "max_attempts,version,created_at,updated_at,available_at) "
            "VALUES ('c1','k-c','{}','running',0,1,0,1,1,0)"
        )
    e, _ = store.enqueue("e")
    te, toke = claim(store, "w", lease_seconds=10)
    clock.advance(11)

    results = {(t.id): o for t, o in store.recover_stuck()}
    assert results["c1"] is RecoverOutcome.QUARANTINED
    assert results[e.id] is RecoverOutcome.REQUEUED

    quarantined = store.get("c1")
    assert quarantined.status is TaskStatus.QUARANTINED
    assert quarantined.lease_owner is None and quarantined.lease_token is None
    log_task("异常任务已隔离", quarantined)

    requeued = store.get(e.id)
    assert requeued.status is TaskStatus.PENDING and requeued.available_at <= clock.now
    # 隔离任务不能直接提交
    with pytest.raises(TaskRejected) as exc:
        store.complete("c1", "w", "anything")
    assert exc.value.reason is RejectReason.TASK_QUARANTINED

    # 人工确认后重新放回队列
    back = store.requeue_quarantined("c1")
    assert back.status is TaskStatus.PENDING


def test_recovery_is_idempotent(store, clock) -> None:
    e, _ = store.enqueue("e")
    te, toke = claim(store, "w", 10)
    clock.advance(11)
    first = store.recover_stuck()
    assert len(first) == 1
    version_after = store.get(e.id).version
    # 反复执行：无候选，零副作用（version 不再变化）
    assert store.recover_stuck() == []
    assert store.recover_stuck(dry_run=False) == []
    assert store.get(e.id).version == version_after


def test_recovery_dry_run_has_no_side_effects(store, clock) -> None:
    with store._conn:  # type: ignore[attr-defined]
        store._conn.execute(  # type: ignore[attr-defined]
            "INSERT INTO tasks (id,idempotency_key,payload,status,attempt,"
            "max_attempts,version,created_at,updated_at,available_at) "
            "VALUES ('c1','','{}','running',0,1,0,1,1,0)"
        )
    planned = store.recover_stuck(dry_run=True)
    assert {(t.id): o for t, o in planned}["c1"] is RecoverOutcome.QUARANTINED
    assert store.get("c1").status is TaskStatus.RUNNING


def test_recovery_never_touches_active_lease(store, clock) -> None:
    a, _ = store.enqueue("a")
    ta, toka = claim(store, "w", lease_seconds=1000)
    # 即便显式传该任务 id，active 也只会被跳过
    results = store.recover_stuck(a.id)
    assert results == [(ta, RecoverOutcome.SKIPPED_ACTIVE)] or \
        (len(results) == 1 and results[0][1] is RecoverOutcome.SKIPPED_ACTIVE)
    held = store.get(a.id)
    assert held.status is TaskStatus.RUNNING
    assert held.lease_token == toka
    # 原持有者的提交全程不受影响
    assert store.complete(a.id, "w", toka, {"ok": 1}).status is TaskStatus.SUCCEEDED


def test_corrupt_auto_requeue_mode(tmp_path) -> None:
    """quarantine_corrupt=False 时异常任务也自动重新可领取（兜底策略）。"""
    db = _legacy_db(
        tmp_path, "auto.db", [("x", "running", "k-x"), ("y", "pending", "k-y")]
    )
    st = TaskStore(db)
    results = st.recover_stuck(quarantine_corrupt=False)
    assert {t.id: o for t, o in results}["x"] is RecoverOutcome.REQUEUED
    assert st.get("x").status is TaskStatus.PENDING
    # 两条都变成可领取；按 available_at/created 顺序领完，确认 x 在其中
    acquired_ids = set()
    for _ in range(2):
        claimed = st.acquire("w")
        assert claimed is not None
        acquired_ids.add(claimed[0].id)
    assert acquired_ids == {"x", "y"}
    st.close()


def test_legacy_running_with_missing_fields_then_full_lifecycle(tmp_path) -> None:
    db = _legacy_db(tmp_path, "life.db", [("x", "running", "k-x")])
    st = TaskStore(db)
    st.recover_stuck()
    assert st.get("x").status is TaskStatus.QUARANTINED
    st.requeue_quarantined("x")
    claimed, tok = st.acquire("w", lease_seconds=100)
    assert claimed.id == "x" and claimed.attempt == 1
    done = st.complete("x", "w", tok, {"fixed": True})
    assert done.status is TaskStatus.SUCCEEDED
    assert st.get_result("x")["result"] == {"fixed": True}
    st.close()


def test_recovery_does_not_block_normal_acquire_and_commit(store, clock) -> None:
    """恢复分批短事务：恢复进行中正常领取/提交依旧成功。"""
    # 一个 corrupt 卡住行 + 一个正常可领取行同时存在
    with store._conn:  # type: ignore[attr-defined]
        store._conn.execute(  # type: ignore[attr-defined]
            "INSERT INTO tasks (id,idempotency_key,payload,status,attempt,"
            "max_attempts,version,created_at,updated_at,available_at) "
            "VALUES ('c1','','{}','running',0,1,0,1,1,0)"
        )
    ok_task, _ = store.enqueue("normal")
    store.recover_stuck()  # 恢复 corrupt 行
    claimed, tok = claim(store, "w")
    assert claimed.id == ok_task.id
    assert store.complete(ok_task.id, "w", tok).status is TaskStatus.SUCCEEDED
