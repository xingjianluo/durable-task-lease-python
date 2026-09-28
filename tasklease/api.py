"""与现有 FastAPI 服务共存的任务 HTTP 接口（可选挂载）。

原有 "/" 行为保持不变；新接口统一挂载在 "/tasks" 前缀下。
被拒绝的非法操作返回 HTTP 409，响应体携带可区分的 ``reason`` 码。
"""
from __future__ import annotations

import dataclasses
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from .archive import ArchiveService
from .errors import (
    RejectReason,
    TaskRejected,
)
from .history import HistoryService
from .models import Task
from .recovery import RecoveryService
from .store import TaskStore


class EnqueueRequest(BaseModel):
    idempotency_key: str = Field(min_length=1)
    payload: dict[str, Any] = Field(default_factory=dict)
    max_attempts: int = Field(default=3, ge=1)


class FailRequest(BaseModel):
    consumer: str = Field(min_length=1)
    lease_token: str = Field(min_length=1, description="本次领取获得的执行凭证")
    error: str = ""
    backoff_seconds: float | None = Field(default=None, gt=0)
    expected_version: int | None = None


class CompleteRequest(BaseModel):
    consumer: str = Field(min_length=1)
    lease_token: str = Field(min_length=1, description="本次领取获得的执行凭证")
    result: dict[str, Any] = Field(default_factory=dict)
    expected_version: int | None = None


class LeaseRequest(BaseModel):
    consumer: str = Field(min_length=1)
    lease_seconds: float | None = Field(default=None, gt=0)
    lease_token: str | None = Field(
        default=None,
        description="续期/释放时必带，必须是本次有效租约的执行凭证",
    )


class RecoverRequest(BaseModel):
    requeue_corrupt: bool = False
    limit: int = Field(default=500, ge=1)


class RequeueQuarantinedRequest(BaseModel):
    expected_version: int | None = None


class ArchiveRequest(BaseModel):
    older_than_seconds: float | None = Field(default=None, ge=0)
    limit: int = Field(default=500, ge=1)


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
        "created_at": t.created_at,
        "updated_at": t.updated_at,
        "archived": t.archived,
    }


def _page_view(page: Any, item_fn: Any) -> dict[str, Any]:
    return {
        "items": [item_fn(i) for i in page.items],
        "total": page.total,
        "limit": page.limit,
        "offset": page.offset,
        "next_offset": page.next_offset,
    }


def _event_view(e: Any) -> dict[str, Any]:
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
        grant = st.acquire(req.consumer, req.lease_seconds)
        if grant is None:
            return {"acquired": False, "task": None, "lease_token": None}
        # lease_token 是只对本次执行有效的凭证，与任务快照一起返回。
        return {
            "acquired": True,
            "task": _task_view(grant),
            "lease_token": grant.lease_token,
        }

    @router.post("/{task_id}/complete")
    def complete(
        task_id: str,
        req: CompleteRequest,
        st: TaskStore = Depends(get_store),
    ) -> dict[str, Any]:
        try:
            task = st.complete(
                task_id, req.consumer, req.result, req.expected_version,
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

    # ----- 执行产出 / 历史查询 -----

    @router.get("/{task_id}/result")
    def get_result(
        task_id: str, st: TaskStore = Depends(get_store)
    ) -> dict[str, Any]:
        try:
            result = HistoryService(st).get_result(task_id)
        except TaskRejected as exc:
            raise _reject(exc) from exc
        return {"result": dataclasses.asdict(result)}

    @router.get("/events")
    def list_events(
        task_id: str | None = None,
        status: list[str] | None = Query(default=None),
        start: float | None = None,
        end: float | None = None,
        limit: int = Query(default=50, ge=1, le=1000),
        offset: int = Query(default=0, ge=0),
        st: TaskStore = Depends(get_store),
    ) -> dict[str, Any]:
        try:
            page = HistoryService(st).list_events(
                task_id, statuses=status, start=start, end=end,
                limit=limit, offset=offset,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail={"message": str(exc)})
        return _page_view(page, _event_view)

    @router.get("")
    def list_tasks(
        status: list[str] | None = Query(default=None),
        start: float | None = None,
        end: float | None = None,
        limit: int = Query(default=50, ge=1, le=1000),
        offset: int = Query(default=0, ge=0),
        include_archived: bool = False,
        st: TaskStore = Depends(get_store),
    ) -> dict[str, Any]:
        try:
            page = HistoryService(st).list_tasks(
                statuses=status, start=start, end=end, limit=limit,
                offset=offset, include_archived=include_archived,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail={"message": str(exc)})
        return _page_view(page, _task_view)

    # ----- 运维：异常运行任务恢复 -----

    @router.post("/recovery/scan")
    def recovery_scan(st: TaskStore = Depends(get_store)) -> dict[str, Any]:
        return {"report": dataclasses.asdict(RecoveryService(st).scan())}

    @router.post("/recovery/run")
    def recovery_run(
        req: RecoverRequest, st: TaskStore = Depends(get_store)
    ) -> dict[str, Any]:
        report = RecoveryService(st).recover_all(
            requeue_corrupt=req.requeue_corrupt, limit=req.limit
        )
        return {"report": dataclasses.asdict(report)}

    @router.post("/{task_id}/requeue-quarantined")
    def requeue_quarantined(
        task_id: str,
        req: RequeueQuarantinedRequest,
        st: TaskStore = Depends(get_store),
    ) -> dict[str, Any]:
        try:
            task = RecoveryService(st).requeue_quarantined(
                task_id, expected_version=req.expected_version
            )
        except TaskRejected as exc:
            raise _reject(exc) from exc
        return {"task": _task_view(task)}

    # ----- 运维：终态归档 -----

    @router.post("/archive/run")
    def archive_run(
        req: ArchiveRequest, st: TaskStore = Depends(get_store)
    ) -> dict[str, Any]:
        report = ArchiveService(st).archive_terminal(
            older_than_seconds=req.older_than_seconds, limit=req.limit
        )
        return {"report": dataclasses.asdict(report)}

    @router.post("/{task_id}/archive")
    def archive_one(
        task_id: str, st: TaskStore = Depends(get_store)
    ) -> dict[str, str]:
        return {"outcome": ArchiveService(st).archive_one(task_id).value}

    @router.get("/{task_id}")
    def get_task(
        task_id: str, st: TaskStore = Depends(get_store)
    ) -> dict[str, Any]:
        try:
            return {"task": _task_view(st.get(task_id))}
        except TaskRejected as exc:
            raise _reject(exc) from exc

    return router
