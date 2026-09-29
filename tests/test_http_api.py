"""HTTP 接口端到端：凭证流转、可区分拒绝、恢复、查询与归档。"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from tasklease.api import create_router
from tasklease.store import TaskStore

from .conftest import FakeClock


@pytest.fixture
def client(clock: FakeClock):
    st = TaskStore(
        ":memory:", default_lease_seconds=10.0,
        backoff_base_seconds=1.0, clock=clock,
    )
    from fastapi import FastAPI
    app = FastAPI()
    app.include_router(create_router(st))
    with TestClient(app) as c:
        yield c, st, clock
    st.close()


def test_acquire_returns_token_and_lifecycle(client) -> None:
    c, st, clock = client
    r = c.post("/tasks", json={"idempotency_key": "k1", "payload": {"n": 1}})
    assert r.status_code == 200 and r.json()["outcome"] == "created"

    r = c.post("/tasks/acquire", json={"consumer": "w1", "lease_seconds": 10})
    body = r.json()
    assert body["acquired"] is True
    token = body["lease_token"]
    assert token and body["task"]["lease_token"] == token
    tid = body["task"]["id"]

    # 续期缺 token -> 422
    assert c.post(f"/tasks/{tid}/heartbeat",
                  json={"consumer": "w1"}).status_code == 422
    # 续期成功
    r = c.post(f"/tasks/{tid}/heartbeat",
               json={"consumer": "w1", "lease_token": token, "lease_seconds": 20})
    assert r.json()["outcome"] == "renewed"

    # 伪造 token -> 409 invalid_token
    r = c.post(f"/tasks/{tid}/complete",
               json={"consumer": "w1", "lease_token": "forged", "result": {}})
    assert r.status_code == 409 and r.json()["detail"]["reason"] == "invalid_token"

    # 正确 token 完成
    r = c.post(f"/tasks/{tid}/complete",
               json={"consumer": "w1", "lease_token": token, "result": {"ok": 1}})
    assert r.status_code == 200 and r.json()["task"]["status"] == "succeeded"


def test_distinct_reasons_over_http(client) -> None:
    c, st, clock = client
    c.post("/tasks", json={"idempotency_key": "k"})
    b = c.post("/tasks/acquire", json={"consumer": "w", "lease_seconds": 10}).json()
    tid, tok = b["task"]["id"], b["lease_token"]

    # 未持有者 -> lease_held_by_other
    r = c.post(f"/tasks/{tid}/complete",
               json={"consumer": "intruder", "lease_token": tok})
    assert r.json()["detail"]["reason"] == "lease_held_by_other"

    # 过期 -> lease_expired
    clock.advance(11)
    r = c.post(f"/tasks/{tid}/complete",
               json={"consumer": "w", "lease_token": tok, "result": {}})
    assert r.json()["detail"]["reason"] == "lease_expired"

    # 同名消费者重新领取 -> 旧 token invalid_token
    b2 = c.post("/tasks/acquire", json={"consumer": "w", "lease_seconds": 10}).json()
    tok2 = b2["lease_token"]
    r = c.post(f"/tasks/{tid}/complete",
               json={"consumer": "w", "lease_token": tok, "result": {"old": 1}})
    assert r.json()["detail"]["reason"] == "invalid_token"
    # 新 token 成功
    r = c.post(f"/tasks/{tid}/complete",
               json={"consumer": "w", "lease_token": tok2, "result": {"new": 1}})
    assert r.status_code == 200


def test_result_events_list_filter_and_archive_over_http(client) -> None:
    c, st, clock = client
    c.post("/tasks", json={"idempotency_key": "k1"})
    b = c.post("/tasks/acquire", json={"consumer": "w", "lease_seconds": 100}).json()
    tid, tok = b["task"]["id"], b["lease_token"]
    c.post(f"/tasks/{tid}/complete",
           json={"consumer": "w", "lease_token": tok, "result": {"v": 9}})

    # 结果读回
    r = c.get(f"/tasks/{tid}/result").json()
    assert r["result"] == {"v": 9} and r["status"] == "succeeded"

    # 历史读回
    evs = c.get(f"/tasks/{tid}/events").json()["events"]
    assert [e["event"] for e in evs] == ["enqueue.created", "acquire", "complete"]
    assert evs[1]["lease_token"] == tok

    # 列表 + 状态筛选 + 分页
    for i in range(3):
        c.post("/tasks", json={"idempotency_key": f"p{i}"})
    r = c.get("/tasks", params={"status": "pending", "limit": 2}).json()
    assert len(r["tasks"]) == 2 and r["next_cursor"] is not None
    r2 = c.get("/tasks", params={"status": "pending", "limit": 2,
                                 "cursor": r["next_cursor"]}).json()
    assert len(r2["tasks"]) == 1
    # 时间范围筛选：未来窗口为空
    r3 = c.get("/tasks", params={"created_from": clock.now + 10}).json()
    assert r3["tasks"] == []

    # 归档（太新不动；推时钟后归档）
    assert c.post("/tasks/admin/archive",
                  params={"min_age_seconds": 10}).json()["archived"] == 0
    clock.advance(11)
    assert c.post("/tasks/admin/archive",
                  params={"min_age_seconds": 10}).json()["archived"] == 1
    # 归档后仍可读结果与历史
    assert c.get(f"/tasks/{tid}/result").json()["archived"] is True
    assert c.get(f"/tasks/{tid}").json()["task"]["status"] == "succeeded"
    # 归档表列表
    arch = c.get("/tasks/archive/list", params={"status": "succeeded"}).json()
    assert len(arch["tasks"]) == 1
    # 重复入队结论不变
    r = c.post("/tasks", json={"idempotency_key": "k1"})
    assert r.status_code == 409 and r.json()["detail"]["reason"] == "task_terminal"


def test_admin_recover_over_http(client) -> None:
    c, st, clock = client
    # 造一条缺租约字段的 legacy running 行
    import sqlite3
    with st._conn:  # type: ignore[attr-defined]
        st._conn.execute(  # type: ignore[attr-defined]
            "INSERT INTO tasks (id,idempotency_key,payload,status,attempt,"
            "max_attempts,version,created_at,updated_at,available_at) "
            "VALUES ('c1','k-c','{}','running',0,1,0,1,1,0)"
        )
    # stuck 列表把它标出 corrupt
    r = c.get("/tasks/admin/stuck").json()
    assert r["stuck"][0]["state"] == "corrupt"

    # dry-run 不动数据
    r = c.post("/tasks/admin/recover", params={"dry_run": "true"}).json()
    assert r["results"][0]["state_outcome"] == "quarantined"
    assert c.get("/tasks/c1").json()["task"]["status"] == "running"

    # 真正隔离
    r = c.post("/tasks/admin/recover").json()
    assert r["results"][0]["state_outcome"] == "quarantined"
    assert c.get("/tasks/c1").json()["task"]["status"] == "quarantined"
    # 再次执行：幂等，无新增结果
    assert c.post("/tasks/admin/recover").json()["results"] == []
    # 人工 requeue
    r = c.post("/tasks/admin/recover/c1/requeue")
    assert r.status_code == 200
    assert c.get("/tasks/c1").json()["task"]["status"] == "pending"


def test_404_for_missing_task(client) -> None:
    c, _, _ = client
    assert c.get("/tasks/nope").status_code == 404
    r = c.post("/tasks/nope/complete",
               json={"consumer": "w", "lease_token": "t"})
    assert r.status_code == 404
