"""HTTP API 集成测试：根路由行为不变 + 任务接口/拒绝原因/并发结论。"""

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from task_store.api.routes import create_router
from task_store.clock import FakeClock
from task_store.service import DurableTaskService


@pytest.fixture()
def client(tmp_path):
    clock = FakeClock(1000.0)
    svc = DurableTaskService(str(tmp_path / "api.db"), clock,
                             lease_seconds=10, default_max_attempts=1)
    app = FastAPI()
    app.include_router(create_router(svc))

    @app.get("/")
    def root():
        return {"Hello": "World"}

    with TestClient(app) as c:
        yield c, svc, clock


def test_root_route_unchanged(client):
    c, _, _ = client
    assert c.get("/").json() == {"Hello": "World"}


def test_enqueue_dedupe_and_conflicts(client):
    c, _, _ = client
    r1 = c.post("/api/tasks", json={"idempotency_key": "k1", "payload": "p"})
    assert r1.status_code == 201 and r1.json()["outcome"] == "CREATED"
    r2 = c.post("/api/tasks", json={"idempotency_key": "k1"})
    assert r2.status_code == 201 and r2.json()["outcome"] == "DEDUPED_PENDING"
    assert r1.json()["task"]["id"] == r2.json()["task"]["id"]

    # 空幂等键 -> 422
    assert c.post("/api/tasks", json={"idempotency_key": ""}).status_code == 422


def test_full_lease_lifecycle_and_distinguishable_409(client):
    c, _, clock = client
    c.post("/api/tasks", json={"idempotency_key": "job"})

    # c1 领取
    r = c.post("/api/tasks/claim", json={"owner": "c1"}).json()
    assert r["outcome"] == "ACQUIRED"
    tid, token = r["lease"]["task_id"], r["lease"]["fencing_token"]

    # 无任务可领
    assert c.post("/api/tasks/claim", json={"owner": "c2"}).json()["outcome"] == "EMPTY"

    # 重复入队 -> 409 ALREADY_RUNNING（原因可区分）
    resp = c.post("/api/tasks", json={"idempotency_key": "job"})
    assert resp.status_code == 409
    assert resp.json()["detail"]["reason"] == "ALREADY_RUNNING"

    # 别人续期 -> RENEWED? 否：HELD_BY_OTHER
    resp = c.post(f"/api/tasks/{tid}/renew",
                  json={"owner": "cx", "fencing_token": token})
    assert resp.json()["result"] == "HELD_BY_OTHER"

    # 过期
    clock.advance(10.1)
    resp = c.post(f"/api/tasks/{tid}/renew",
                  json={"owner": "c1", "fencing_token": token})
    assert resp.json()["result"] == "LEASE_EXPIRED"

    # c1 持过期租约提交 -> LEASE_EXPIRED
    resp = c.post(f"/api/tasks/{tid}/complete",
                  json={"owner": "c1", "fencing_token": token})
    assert resp.json()["result"] == "LEASE_EXPIRED"

    # c2 回收重领
    r = c.post("/api/tasks/claim", json={"owner": "c2"}).json()
    assert r["outcome"] == "EXPIRED_REACQUIRED"
    token2 = r["lease"]["fencing_token"]
    assert token2 == token + 1

    # 旧 token 提交 -> HELD_BY_OTHER
    resp = c.post(f"/api/tasks/{tid}/complete",
                  json={"owner": "c1", "fencing_token": token})
    assert resp.json()["result"] == "HELD_BY_OTHER"

    # 失败达到上限 -> FAILED 终态
    resp = c.post(f"/api/tasks/{tid}/fail",
                  json={"owner": "c2", "fencing_token": token2,
                        "error_message": "x"})
    assert resp.json()["result"] == "EXHAUSTED"
    assert resp.json()["task"]["status"] == "FAILED"

    # 终态重复入队 -> 409 ALREADY_TERMINAL
    resp = c.post("/api/tasks", json={"idempotency_key": "job"})
    assert resp.status_code == 409
    assert resp.json()["detail"]["reason"] == "ALREADY_TERMINAL"

    # 404
    assert c.get("/api/tasks/nope").status_code == 404


def test_version_conflict_rejected(client):
    c, _, _ = client
    c.post("/api/tasks", json={"idempotency_key": "v"})
    r = c.post("/api/tasks/claim", json={"owner": "c1"}).json()
    tid, token = r["lease"]["task_id"], r["lease"]["fencing_token"]
    version = c.get(f"/api/tasks/{tid}").json()["version"]
    # 续期改变版本
    c.post(f"/api/tasks/{tid}/renew", json={"owner": "c1", "fencing_token": token})
    resp = c.post(f"/api/tasks/{tid}/complete",
                  json={"owner": "c1", "fencing_token": token,
                        "expected_version": version})
    assert resp.status_code == 409
    assert resp.json()["detail"]["reason"] == "VERSION_CONFLICT"
