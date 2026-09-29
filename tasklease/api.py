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
    RejectReason,
    RunningLeaseState,
    TaskRejected,
)
from .models import Task, TaskStatus
from .store import TaskStore


class EnqueueRequest(BaseModel):
    idempotency_key: str = Field(min_length=1)
    payload: dict[str, Any] = Field(default_factory=dict)
    max_attempts: int = Field(default=3, ge=1)


class FailRequest(BaseModel):
    consumer: str = Field(min_length=1)
    lease_token: str = Field(min_length=1, description="领取时发放的本次执行凭证")
    error: str = ""
    backoff_seconds: float | None = Field(default=None, gt=0)
    expected_version: int | None = None


class CompleteRequest(BaseModel):
    consumer: str = Field(min_length=1)
    lease_token: str = Field(min_length=1, description="领取时发放的本次执行凭证")
    result: dict[str, Any] = Field(default_factory=dict)
    expected_version: int | None = None


class LeaseRequest(BaseModel):
    consumer: str = Field(min_length=1)
    lease_token: str | None = Field(
        default=None, description="续期/释放时必须携带领取时发放的凭证"
    )
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
        "lease_token": t.lease_token,
        "available_at": t.available_at,
        "last_error": t.last_error,
        "result": t.result,
        "archived_at": t.archived_at,
        "created_at": t.created_at,
        "updated_at": t.updated_at,
    }


def _event_view(e) -> dict[str, Any]:
    return {
        "id": e.id,
        "task_id": e.task_id,
        "event": e.event,
        "actor": e.actor,
        "lease_token": e.lease_token,
        "from_status": e.from_status,
        "to_status": e.to_status,
        "version": e.version,
        "reason": e.reason,
        "created_at": e.created_at,
        "archived": e.archived,
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
        claimed = st.acquire(req.consumer, req.lease_seconds)
        if claimed is None:
            return {"acquired": False, "task": None, "lease_token": None}
        task, lease_token = claimed
        # lease_token 同时出现在顶层与任务视图里：顶层方便调用方直接取用。
        return {"acquired": True, "task": _task_view(task), "lease_token": lease_token}

    @router.post("/{task_id}/complete")
    def complete(
        task_id: str,
        req: CompleteRequest,
        st: TaskStore = Depends(get_store),
    ) -> dict[str, Any]:
        try:
            task = st.complete(
                task_id, req.consumer, req.lease_token,
                req.result, req.expected_version,
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
                req.lease_token,
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
        if not req.lease_token:
            raise HTTPException(
                status_code=422,
                detail={"reason": "missing_token",
                        "message": "heartbeat 必须携带 lease_token"},
            )
        outcome = st.heartbeat(
            task_id, req.consumer, req.lease_token, req.lease_seconds
        )
        return {"outcome": outcome.value}

    @router.post("/{task_id}/release")
    def release(
        task_id: str,
        req: LeaseRequest,
        st: TaskStore = Depends(get_store),
    ) -> dict[str, str]:
        if not req.lease_token:
            raise HTTPException(
                status_code=422,
                detail={"reason": "missing_token",
                        "message": "release 必须携带 lease_token"},
            )
        outcome = st.release(task_id, req.consumer, req.lease_token)
        return {"outcome": outcome.value}

    # ---------- 结果与执行历史读回 ----------

    @router.get("/{task_id}/result")
    def get_result(
        task_id: str,
        include_archive: bool = True,
        st: TaskStore = Depends(get_store),
    ) -> dict[str, Any]:
        try:
            return st.get_result(task_id, include_archive=include_archive)
        except TaskRejected as exc:
            raise _reject(exc) from exc

    @router.get("/{task_id}/events")
    def get_events(
        task_id: str,
        limit: int = 100,
        include_archive: bool = True,
        st: TaskStore = Depends(get_store),
    ) -> dict[str, Any]:
        events = st.list_events(task_id, limit=limit, include_archive=include_archive)
        return {"task_id": task_id, "events": [_event_view(e) for e in events]}

    @router.get("")
    def list_tasks(
        status: str | None = None,
        updated_from: float | None = None,
        updated_to: float | None = None,
        created_from: float | None = None,
        created_to: float | None = None,
        limit: int = 50,
        cursor: int | None = None,
        st: TaskStore = Depends(get_store),
    ) -> dict[str, Any]:
        statuses: list[TaskStatus] | None = None
        if status:
            try:
                statuses = [TaskStatus(s) for s in status.split(",") if s]
            except ValueError as exc:
                raise HTTPException(
                    status_code=422,
                    detail={"reason": "bad_status", "message": str(exc)},
                ) from exc
        page = st.list_tasks(
            statuses=statuses, updated_from=updated_from, updated_to=updated_to,
            created_from=created_from, created_to=created_to,
            limit=limit, cursor=cursor,
        )
        return {
            "tasks": [_task_view(t) for t in page.items],
            "limit": page.limit,
            "next_cursor": page.next_cursor,
        }

    @router.get("/archive/list")
    def list_archive(
        status: str | None = None,
        updated_from: float | None = None,
        updated_to: float | None = None,
        created_from: float | None = None,
        created_to: float | None = None,
        limit: int = 50,
        cursor: int | None = None,
        st: TaskStore = Depends(get_store),
    ) -> dict[str, Any]:
        statuses: list[TaskStatus] | None = None
        if status:
            statuses = [TaskStatus(s) for s in status.split(",") if s]
        page = st.list_archived_tasks(
            statuses=statuses, updated_from=updated_from, updated_to=updated_to,
            created_from=created_from, created_to=created_to,
            limit=limit, cursor=cursor,
        )
        return {
            "tasks": [_task_view(t) for t in page.items],
            "limit": page.limit,
            "next_cursor": page.next_cursor,
        }

    # ---------- 运维：异常 RUNNING 恢复 / 归档 ----------

    @router.get("/admin/stuck")
    def list_stuck(
        limit: int = 500, st: TaskStore = Depends(get_store)
    ) -> dict[str, Any]:
        tasks = st.list_stuck_running(limit=limit)
        items: list[dict[str, Any]] = []
        for t in tasks:
            if not t.lease_owner or not t.lease_token or t.lease_expires_at is None:
                state = RunningLeaseState.CORRUPT.value
            else:
                state = RunningLeaseState.EXPIRED.value
            items.append({"state": state, "task": _task_view(t)})
        return {"stuck": items}

    @router.post("/admin/recover")
    def recover(
        task_id: str | None = None,
        batch_size: int = 100,
        dry_run: bool = False,
        quarantine_corrupt: bool = True,
        st: TaskStore = Depends(get_store),
    ) -> dict[str, Any]:
        results = st.recover_stuck(
            task_id,
            batch_size=batch_size,
            dry_run=dry_run,
            quarantine_corrupt=quarantine_corrupt,
        )
        return {
            "dry_run": dry_run,
            "results": [
                {"task_id": t.id, "state_outcome": o.value, "task": _task_view(t)}
                for t, o in results
            ],
        }

    @router.post("/admin/recover/{task_id}/requeue")
    def requeue_quarantined(
        task_id: str, st: TaskStore = Depends(get_store)
    ) -> dict[str, Any]:
        try:
            task = st.requeue_quarantined(task_id)
        except TaskRejected as exc:
            raise _reject(exc) from exc
        return {"task": _task_view(task)}

    @router.post("/admin/archive")
    def archive(
        min_age_seconds: float = 86400.0,
        batch_size: int = 200,
        dry_run: bool = False,
        st: TaskStore = Depends(get_store),
    ) -> dict[str, int | bool]:
        count = st.archive_terminal(
            min_age_seconds=min_age_seconds,
            batch_size=batch_size,
            dry_run=dry_run,
        )
        return {"archived": count, "dry_run": dry_run}

    @router.get("/{task_id}")
    def get_task(
        task_id: str,
        include_archive: bool = True,
        st: TaskStore = Depends(get_store),
    ) -> dict[str, Any]:
        try:
            return {"task": _task_view(st.get(task_id, include_archive))}
        except TaskRejected as exc:
            raise _reject(exc) from exc

    return router
