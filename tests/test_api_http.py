"""HTTP 接口集成测试：执行凭证透传、查询分页、运维入口与拒绝码。"""
from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from tasklease.api import create_router
from tasklease.store import TaskStore


@pytest.fixture
def client(tmp_path) -> TestClient:
    st = TaskStore(str(tmp_path / "api.db"))
    app = FastAPI()
    app.include_router(create_router(st))
    with TestClient(app) as c:
        c._store = st  # type: ignore[attr-defined]
        yield c


def test_acquire_returns_token_and_guards_ops(client: TestClient) -> None:
    r = client.post("/tasks", json={"idempotency_key": "api-1"})
    assert r.status_code == 200
    r = client.post("/tasks/acquire", json={"consumer": "c1", "lease_seconds": 10})
    body = r.json()
    assert body["acquired"] is True and body["lease_token"]
    tid, token = body["task"]["id"], body["lease_token"]

    # 错凭证：续期/释放被明确拒绝
    assert client.post(
        f"/tasks/{tid}/heartbeat",
        json={"consumer": "c1", "lease_token": "bogus"},
    ).json()["outcome"] == "invalid_token"
    assert client.post(
        f"/tasks/{tid}/release",
        json={"consumer": "c1", "lease_token": "bogus"},
    ).json()["outcome"] == "invalid_token"

    # 不带凭证（兼容路径）可续期；带正确凭证提交成功
    assert client.post(
        f"/tasks/{tid}/heartbeat", json={"consumer": "c1"}
    ).json()["outcome"] == "renewed"
    r = client.post(
        f"/tasks/{tid}/complete",
        json={"consumer": "c1", "result": {"answer": 42}, "lease_token": token},
    )
    assert r.status_code == 200

    r = client.get(f"/tasks/{tid}/result")
    assert r.json()["result"]["result"] == {"answer": 42}


def test_old_token_overwrite_returns_409_distinct_reason(
    client: TestClient, monkeypatch
) -> None:
    # 用可控时钟迫使租约过期，然后同名消费者重新领取
    class FixedClock:
        def __init__(self) -> None:
            self.t = 1000.0

        def __call__(self) -> float:
            return self.t

    clock = FixedClock()
    st = TaskStore(":memory:", default_lease_seconds=10, clock=clock)
    app = FastAPI(); app.include_router(create_router(st))
    c = TestClient(app)
    c.post("/tasks", json={"idempotency_key": "api-rot"})
    g = c.post("/tasks/acquire", json={"consumer": "dup", "lease_seconds": 10}).json()
    clock.t += 11
    g2 = c.post("/tasks/acquire", json={"consumer": "dup", "lease_seconds": 10}).json()
    assert g2["lease_token"] != g["lease_token"]

    r = c.post(
        f"/tasks/{g['task']['id']}/complete",
        json={"consumer": "dup", "result": {"old": 1},
              "lease_token": g["lease_token"]},
    )
    assert r.status_code == 409
    assert r.json()["detail"]["reason"] == "lease_token_mismatch"

    r = c.post(
        f"/tasks/{g2['task']['id']}/complete",
        json={"consumer": "dup", "result": {"new": 1},
              "lease_token": g2["lease_token"]},
    )
    assert r.status_code == 200


def test_events_and_tasks_listing_pagination_filters(client: TestClient) -> None:
    for i in range(3):
        client.post("/tasks", json={"idempotency_key": f"api-list-{i}"})
        b = client.post("/tasks/acquire", json={"consumer": "w"}).json()
        client.post(
            f"/tasks/{b['task']['id']}/complete",
            json={"consumer": "w", "lease_token": b["lease_token"]},
        )

    r = client.get("/tasks/events", params={"status": "succeeded", "limit": 2})
    j = r.json()
    assert len(j["items"]) == 2 and j["next_cursor"]
    nxt = client.get(
        "/tasks/events",
        params={"status": "succeeded", "limit": 2, "cursor": j["next_cursor"]},
    ).json()
    assert len(nxt["items"]) == 1 and nxt["next_cursor"] is None

    # 任务列表 + 状态过滤
    r = client.get("/tasks", params={"status": "succeeded"})
    assert len(r.json()["items"]) == 3
    # 非法状态码 -> 400
    assert client.get("/tasks", params={"status": "bogus"}).status_code == 400
    # 非法 cursor -> 400
    assert client.get("/tasks", params={"cursor": "@@@"}).status_code == 400


def test_admin_recover_and_archive_endpoints(client: TestClient) -> None:
    # 造一个异常 RUNNING：直接改库
    b = client.post("/tasks", json={"idempotency_key": "api-stuck"}).json()
    g = client.post("/tasks/acquire", json={"consumer": "s"}).json()
    tid = g["task"]["id"]
    store: TaskStore = client._store  # type: ignore[attr-defined]
    store._conn.execute(  # type: ignore[attr-defined]
        "UPDATE tasks SET lease_expires_at=NULL WHERE id=?", (tid,)
    )
    store._conn.commit()  # type: ignore[attr-defined]

    h = client.get(f"/tasks/{tid}/lease-health")
    assert h.json()["health"] == "malformed"

    r = client.post(
        "/tasks/admin/recover-running", json={"task_ids": [tid]}
    )
    assert r.status_code == 200
    assert r.json()["recovered"] == 1
    assert r.json()["items"][0]["health"] == "malformed"

    # 恢复后正常走完并归档
    g2 = client.post("/tasks/acquire", json={"consumer": "fixed"}).json()
    assert g2["acquired"] and g2["task"]["id"] == tid
    client.post(
        f"/tasks/{tid}/complete",
        json={"consumer": "fixed", "lease_token": g2["lease_token"]},
    )
    r = client.post("/tasks/admin/archive-terminal", json={"limit": 10})
    assert r.json()["archived"] == 1
    # 归档后重复入队被拒
    r = client.post("/tasks", json={"idempotency_key": "api-stuck"})
    assert r.status_code == 409
    # 归档后仍可读
    assert client.get(f"/tasks/{tid}/result").json()["result"]["archived"] is True


def test_unknown_task_maps_to_404(client: TestClient) -> None:
    assert client.get("/tasks/nope").status_code == 404
