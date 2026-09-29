"""与现有 FastAPI 服务共存的任务 HTTP 接口（可选挂载）。

原有 "/" 行为保持不变；任务接口统一挂载在 "/tasks" 前缀下。
被拒绝的非法操作返回 HTTP 409（任务不存在 404），响应体携带可区分的
``reason`` 码；非法分页参数返回 400。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from .errors import (
    RecoveryAction,
    RejectReason,
    TaskRejected,
)
from .models import Task, TaskEvent, TaskStatus
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
    lease_token: str | None = None


class CompleteRequest(BaseModel):
    consumer: str = Field(min_length=1)
    result: dict[str, Any] = Field(default_factory=dict)
    expected_version: int | None = None
    lease_token: str | None = None


class LeaseRequest(BaseModel):
    consumer: str = Field(min_length=1)
    lease_seconds: float | None = Field(default=None, gt=0)
    lease_token: str | None = None


class RecoverRequest(BaseModel):
    action: RecoveryAction = RecoveryAction.REQUEUE
    task_ids: list[str] | None = None
    limit: int = Field(default=100, ge=1, le=10_000)


class ArchiveRequest(BaseModel):
    older_than_seconds: float | None = Field(default=None, gt=0)
    limit: int = Field(default=100, ge=1, le=10_000)


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
        "archived": t.archived,
        "archived_at": t.archived_at,
    }


def _event_view(e: TaskEvent) -> dict[str, Any]:
    return {
        "id": e.id,
        "task_id": e.task_id,
        "event": e.event,
        "actor": e.actor,
        "from_status": e.from_status,
        "to_status": e.to_status,
        "version": e.version,
        "reason": e.reason,
        "created_at": e.created_at,
        "lease_token": e.lease_token,
        "archived": e.archived,
    }


def _reject(exc: TaskRejected) -> HTTPException:
    status_code = 404 if exc.reason is RejectReason.TASK_NOT_FOUND else 409
    return HTTPException(
        status_code=status_code,
        detail={"reason": exc.reason.value, "message": str(exc)},
    )


def _bad_cursor() -> HTTPException:
    return HTTPException(status_code=400, detail={"reason": "bad_cursor"})


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
        grant = st.acquire_grant(req.consumer, req.lease_seconds)
        if grant is None:
            return {"acquired": False, "task": None, "lease_token": None}
        # lease_token 是只属于本次执行的凭证，后续操作必须原样回传。
        return {
            "acquired": True,
            "task": _task_view(grant.task),
            "lease_token": grant.token,
        }

    # --- 查询类接口（放在 /{task_id} 之前，避免路径被吞） ---

    @router.get("")
    def list_tasks(
        status: list[str] | None = Query(default=None),
        since: float | None = None,
        until: float | None = None,
        limit: int = Query(default=50, ge=1, le=500),
        cursor: str | None = None,
        include_archived: bool = False,
        st: TaskStore = Depends(get_store),
    ) -> dict[str, Any]:
        statuses = None
        if status:
            try:
                statuses = [TaskStatus(s) for s in status]
            except ValueError as exc:
                raise HTTPException(
                    status_code=400, detail={"reason": "bad_status"}
                ) from exc
        try:
            page = st.list_tasks(
                statuses=statuses,
                since=since,
                until=until,
                limit=limit,
                cursor=cursor,
                include_archived=include_archived,
            )
        except ValueError:
            raise _bad_cursor()
        return {
            "items": [_task_view(t) for t in page.items],
            "next_cursor": page.next_cursor,
        }

    @router.get("/events")
    def list_events(
        task_id: str | None = None,
        status: list[str] | None = Query(default=None),
        actor: str | None = None,
        since: float | None = None,
        until: float | None = None,
        limit: int = Query(default=50, ge=1, le=500),
        cursor: str | None = None,
        include_archived: bool = True,
        st: TaskStore = Depends(get_store),
    ) -> dict[str, Any]:
        statuses = None
        if status:
            valid = {s.value for s in TaskStatus}
            if any(s not in valid for s in status):
                raise HTTPException(status_code=400, detail={"reason": "bad_status"})
            statuses = status
        try:
            page = st.list_events(
                task_id,
                statuses=statuses,
                actor=actor,
                since=since,
                until=until,
                limit=limit,
                cursor=cursor,
                include_archived=include_archived,
            )
        except ValueError:
            raise _bad_cursor()
        return {
            "items": [_event_view(e) for e in page.items],
            "next_cursor": page.next_cursor,
        }

    # --- 运维入口 ---

    @router.post("/admin/recover-running")
    def recover_running(
        req: RecoverRequest, st: TaskStore = Depends(get_store)
    ) -> dict[str, Any]:
        try:
            report = st.recover_running(
                req.action, task_ids=req.task_ids, limit=req.limit
            )
        except TaskRejected as exc:
            raise _reject(exc) from exc
        return {
            "scanned": report.scanned,
            "recovered": report.recovered,
            "skipped_held": report.skipped_held,
            "items": [
                {
                    "task_id": i.task_id,
                    "result": i.result.value,
                    "health": i.health.value if i.health else None,
                }
                for i in report.items
            ],
        }

    @router.post("/admin/archive-terminal")
    def archive_terminal(
        req: ArchiveRequest, st: TaskStore = Depends(get_store)
    ) -> dict[str, Any]:
        report = st.archive_terminal(
            older_than=req.older_than_seconds, limit=req.limit
        )
        return {
            "archived": report.archived,
            "scanned": report.scanned,
            "task_ids": list(report.task_ids),
        }

    # --- 单任务操作 ---

    @router.post("/{task_id}/complete")
    def complete(
        task_id: str,
        req: CompleteRequest,
        st: TaskStore = Depends(get_store),
    ) -> dict[str, Any]:
        try:
            task = st.complete(
                task_id,
                req.consumer,
                req.result,
                req.expected_version,
                req.lease_token,
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
                req.lease_token,
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
        outcome = st.heartbeat(
            task_id, req.consumer, req.lease_seconds, req.lease_token
        )
        return {"outcome": outcome.value}

    @router.post("/{task_id}/release")
    def release(
        task_id: str,
        req: LeaseRequest,
        st: TaskStore = Depends(get_store),
    ) -> dict[str, str]:
        outcome = st.release(task_id, req.consumer, req.lease_token)
        return {"outcome": outcome.value}

    @router.get("/{task_id}/result")
    def get_result(
        task_id: str, st: TaskStore = Depends(get_store)
    ) -> dict[str, Any]:
        try:
            r = st.get_result(task_id)
        except TaskRejected as exc:
            raise _reject(exc) from exc
        return {
            "result": {
                "task_id": r.task_id,
                "idempotency_key": r.idempotency_key,
                "status": r.status.value,
                "result": r.result,
                "last_error": r.last_error,
                "attempt": r.attempt,
                "max_attempts": r.max_attempts,
                "created_at": r.created_at,
                "updated_at": r.updated_at,
                "archived": r.archived,
            }
        }

    @router.get("/{task_id}/lease-health")
    def lease_health(
        task_id: str, st: TaskStore = Depends(get_store)
    ) -> dict[str, str]:
        try:
            health = st.inspect_lease_health(task_id)
        except TaskRejected as exc:
            raise _reject(exc) from exc
        return {"task_id": task_id, "health": health.value}

    @router.get("/{task_id}")
    def get_task(
        task_id: str,
        include_archived: bool = True,
        st: TaskStore = Depends(get_store),
    ) -> dict[str, Any]:
        try:
            return {"task": _task_view(st.get(task_id, include_archived=include_archived))}
        except TaskRejected as exc:
            raise _reject(exc) from exc

    return router
