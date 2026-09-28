"""HTTP 接口集成：执行凭证强制、结果/历史/恢复/归档端点。"""
from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from tasklease.api import create_router
from tasklease.store import TaskStore


@pytest.fixture
def client(tmp_path) -> TestClient:
    st = TaskStore(str(tmp_path / "http.db"), default_lease_seconds=10.0)
    app = FastAPI()
    app.include_router(create_router(st))
    with TestClient(app) as c:
        c.st = st  # type: ignore[attr-defined]
        yield c


def test_full_lifecycle_with_token_over_http(client: TestClient) -> None:
    r = client.post("/tasks", json={"idempotency_key": "http-1"})
    assert r.status_code == 200

    acq = client.post("/tasks/acquire", json={"consumer": "w1"}).json()
    assert acq["acquired"] is True
    assert acq["lease_token"]
    task_id = acq["task"]["id"]
    token = acq["lease_token"]

    # 不带凭证提交 -> 422（请求体校验）
    r = client.post(f"/tasks/{task_id}/complete",
                    json={"consumer": "w1", "result": {}})
    assert r.status_code == 422

    # 错误凭证 -> 409 token_mismatch
    r = client.post(f"/tasks/{task_id}/complete",
                    json={"consumer": "w1", "lease_token": "z" * 32})
    assert r.status_code == 409
    assert r.json()["detail"]["reason"] == "token_mismatch"

    # 正确凭证完成
    r = client.post(f"/tasks/{task_id}/complete",
                    json={"consumer": "w1", "lease_token": token,
                          "result": {"ok": True}})
    assert r.status_code == 200

    # 结果读回
    res = client.get(f"/tasks/{task_id}/result").json()["result"]
    assert res["result"] == {"ok": True} and res["status"] == "succeeded"


def test_heartbeat_release_require_token_over_http(client: TestClient) -> None:
    client.post("/tasks", json={"idempotency_key": "http-2"})
    acq = client.post("/tasks/acquire", json={"consumer": "w"}).json()
    tid, token = acq["task"]["id"], acq["lease_token"]

    assert client.post(f"/tasks/{tid}/heartbeat",
                       json={"consumer": "w", "lease_seconds": 20,
                             "lease_token": token}).json()["outcome"] == "renewed"
    assert client.post(f"/tasks/{tid}/release",
                       json={"consumer": "w",
                             "lease_token": "x" * 32}).json()["outcome"] == \
        "token_mismatch"
    assert client.post(f"/tasks/{tid}/release",
                       json={"consumer": "w",
                             "lease_token": token}).json()["outcome"] == "released"


def test_recovery_and_archive_endpoints(client: TestClient) -> None:
    st: TaskStore = client.st  # type: ignore[attr-defined]
    # 造一条租约信息异常的 RUNNING 行（旧版本升级风格的不完整行）
    with st._conn:
        st._conn.execute(
            "INSERT INTO tasks (id, idempotency_key, payload, status, attempt, "
            "max_attempts, version, created_at, updated_at, available_at) "
            "VALUES ('bad','','{}','running',0,1,0,1,1,0)"
        )

    scan = client.post("/tasks/recovery/scan").json()["report"]
    assert scan["corrupt"] == 1
    run = client.post("/tasks/recovery/run", json={}).json()["report"]
    assert run["quarantined"] == 1

    # 人工放回
    r = client.post("/tasks/bad/requeue-quarantined", json={})
    assert r.status_code == 200 and r.json()["task"]["status"] == "pending"

    # 完成后归档
    g = client.post("/tasks/acquire", json={"consumer": "w"}).json()
    client.post("/tasks/bad/complete",
                json={"consumer": "w", "lease_token": g["lease_token"]})
    arc = client.post("/tasks/archive/run", json={}).json()["report"]
    assert arc["archived"] == 1

    # 归档后任务仍可 GET / 列事件
    got = client.get("/tasks/bad").json()["task"]
    assert got["archived"] is True and got["status"] == "succeeded"
    ev = client.get("/tasks/events", params={"task_id": "bad"}).json()
    assert ev["total"] >= 2
