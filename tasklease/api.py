"""与现有 FastAPI 服务共存的任务 HTTP 接口（可选挂载）。

原有 "/" 行为保持不变；新接口统一挂载在 "/tasks" 前缀下。
被拒绝的非法操作返回 HTTP 409，响应体携带可区分的 ``reason`` 码。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from .errors import (
    EnqueueOutcome,
    HeartbeatOutcome,
    ReleaseOutcome,
    RejectReason,
    TaskRejected,
)
from .models import Task
from .store import TaskStore


class EnqueueRequest(BaseModel):
    idempotency_key: str = Field(min_length=1)
    payload: dict[str, Any] = Field(default_factory=dict)
    max_attempts: int = Field(default=3, ge=1)


class FailRequest(BaseModel):
    consumer: str = Field(min_length=1)
    error: str = ""
    backoff_seconds: float | None = Field(default=None, gt=0)
    expected_version: int | None = None


class CompleteRequest(BaseModel):
    consumer: str = Field(min_length=1)
    result: dict[str, Any] = Field(default_factory=dict)
    expected_version: int | None = None


class LeaseRequest(BaseModel):
    consumer: str = Field(min_length=1)
    lease_seconds: float | None = Field(default=None, gt=0)


def _task_view(t: Task) -> dict[str, Any]:
    return {
        "id": t.id,
        "idempotency_key": t.idempotency_key,
        "payload": t.payload,
        "status": t.status.value,
        "attempt": t.attempt,
        "max_attempts": t.max_attempts,
        "version": t.version,
        "lease_owner": t.lease_owner,
        "lease_expires_at": t.lease_expires_at,
        "available_at": t.available_at,
        "last_error": t.last_error,
        "created_at": t.created_at,
        "updated_at": t.updated_at,
    }


def _reject(exc: TaskRejected) -> HTTPException:
    status_code = 404 if exc.reason is RejectReason.TASK_NOT_FOUND else 409
    return HTTPException(
        status_code=status_code,
        detail={"reason": exc.reason.value, "message": str(exc)},
    )


def create_router(store: TaskStore) -> APIRouter:
    """根据给定存储构造任务路由。"""
    router = APIRouter(prefix="/tasks", tags=["tasks"])

    def get_store() -> TaskStore:
        return store

    @router.post("")
    def enqueue(req: EnqueueRequest, st: TaskStore = Depends(get_store)) -> dict[str, Any]:
        try:
            task, outcome = st.enqueue(
                req.idempotency_key, req.payload, req.max_attempts
            )
        except TaskRejected as exc:
            raise _reject(exc) from exc
        return {"outcome": outcome.value, "task": _task_view(task)}

    @router.post("/acquire")
    def acquire(
        req: LeaseRequest, st: TaskStore = Depends(get_store)
    ) -> dict[str, Any]:
        task = st.acquire(req.consumer, req.lease_seconds)
        if task is None:
            return {"acquired": False, "task": None}
        return {"acquired": True, "task": _task_view(task)}

    @router.post("/{task_id}/complete")
    def complete(
        task_id: str,
        req: CompleteRequest,
        st: TaskStore = Depends(get_store),
    ) -> dict[str, Any]:
        try:
            task = st.complete(
                task_id, req.consumer, req.result, req.expected_version
            )
        except TaskRejected as exc:
            raise _reject(exc) from exc
        return {"task": _task_view(task)}

    @router.post("/{task_id}/fail")
    def fail(
        task_id: str,
        req: FailRequest,
        st: TaskStore = Depends(get_store),
    ) -> dict[str, Any]:
        try:
            task = st.fail(
                task_id,
                req.consumer,
                req.error,
                req.backoff_seconds,
                req.expected_version,
            )
        except TaskRejected as exc:
            raise _reject(exc) from exc
        return {"task": _task_view(task)}

    @router.post("/{task_id}/heartbeat")
    def heartbeat(
        task_id: str,
        req: LeaseRequest,
        st: TaskStore = Depends(get_store),
    ) -> dict[str, str]:
        outcome = st.heartbeat(task_id, req.consumer, req.lease_seconds)
        return {"outcome": outcome.value}

    @router.post("/{task_id}/release")
    def release(
        task_id: str,
        req: LeaseRequest,
        st: TaskStore = Depends(get_store),
    ) -> dict[str, str]:
        outcome = st.release(task_id, req.consumer)
        return {"outcome": outcome.value}

    @router.get("/{task_id}")
    def get_task(
        task_id: str, st: TaskStore = Depends(get_store)
    ) -> dict[str, Any]:
        try:
            return {"task": _task_view(st.get(task_id))}
        except TaskRejected as exc:
            raise _reject(exc) from exc

    return router
