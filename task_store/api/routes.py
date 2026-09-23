"""FastAPI 路由：在不改变现有根路由行为的前提下挂载到 /api。

拒绝原因映射为可区分的 HTTP 状态/响应体：
- 409 + reason：状态机/租约/版本冲突（body 中 reason 可区分具体原因）；
- 404 + NOT_FOUND：任务不存在；
- 422：请求体校验失败（由 FastAPI 处理）。
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from ..types import RejectReason, StateTransitionError, Task
from .schemas import (
    ClaimRequest,
    ClaimResponse,
    EnqueueRequest,
    FailRequest,
    LeaseActionRequest,
    LeaseHolder,
    ReleaseResponse,
    RenewResponse,
    ResultRequest,
    TaskResponse,
)

_NOT_FOUND = RejectReason.NOT_FOUND


def _error(err: StateTransitionError) -> HTTPException:
    status = 404 if err.reason is _NOT_FOUND else 409
    return HTTPException(
        status_code=status,
        detail={
            "reason": err.reason.value,
            "message": err.message,
            "task_id": err.task_id,
        },
    )


def _lease_holder(lease) -> LeaseHolder:
    return LeaseHolder(
        task_id=lease.task_id,
        owner=lease.owner,
        fencing_token=lease.fencing_token,
        acquired_at=lease.acquired_at,
        expires_at=lease.expires_at,
    )


def _task_resp(task: Task) -> TaskResponse:
    return TaskResponse(
        id=task.id,
        idempotency_key=task.idempotency_key,
        payload=task.payload,
        status=task.status.value,
        available_at=task.available_at,
        lease_owner=task.lease_owner,
        lease_expires_at=task.lease_expires_at,
        fencing_token=task.fencing_token,
        attempt_count=task.attempt_count,
        max_attempts=task.max_attempts,
        version=task.version,
        result_payload=task.result_payload,
        error_message=task.error_message,
        created_at=task.created_at,
        updated_at=task.updated_at,
    )


def create_router(service) -> APIRouter:
    router = APIRouter(prefix="/api", tags=["tasks"])

    @router.post("/tasks", status_code=201)
    def enqueue(req: EnqueueRequest):
        try:
            result = service.enqueue(
                req.idempotency_key,
                req.payload,
                max_attempts=req.max_attempts,
                backoff_base_seconds=req.backoff_base_seconds,
                backoff_factor=req.backoff_factor,
            )
        except StateTransitionError as err:
            raise _error(err) from err
        return {
            "outcome": result.outcome.value,
            "task": _task_resp(result.task).model_dump(),
        }

    @router.post("/tasks/claim", response_model=ClaimResponse)
    def claim(req: ClaimRequest):
        outcome, lease = service.claim(req.owner, lease_seconds=req.lease_seconds)
        if lease is None:
            return ClaimResponse(outcome=outcome.value)
        task = service.get_task(lease.task_id)
        return ClaimResponse(
            outcome=outcome.value,
            lease=_lease_holder(lease),
            task_id=task.id,
            payload=task.payload,
        )

    @router.post("/tasks/{task_id}/renew", response_model=RenewResponse)
    def renew(task_id: str, req: LeaseActionRequest):
        try:
            result, lease = service.renew(
                task_id, req.owner, req.fencing_token,
                lease_seconds=req.lease_seconds,
            )
        except StateTransitionError as err:
            raise _error(err) from err
        return RenewResponse(
            result=result.value,
            lease=_lease_holder(lease) if lease else None,
        )

    @router.post("/tasks/{task_id}/release", response_model=ReleaseResponse)
    def release(task_id: str, req: LeaseActionRequest):
        try:
            result = service.release(task_id, req.owner, req.fencing_token)
        except StateTransitionError as err:
            raise _error(err) from err
        return ReleaseResponse(result=result.value)

    @router.post("/tasks/{task_id}/complete")
    def complete(task_id: str, req: ResultRequest):
        try:
            verdict = service.complete(
                task_id, req.owner, req.fencing_token, req.result_payload,
                expected_version=req.expected_version,
            )
        except StateTransitionError as err:
            raise _error(err) from err
        return {"result": verdict.value}

    @router.post("/tasks/{task_id}/fail")
    def fail(task_id: str, req: FailRequest):
        try:
            verdict, task = service.fail(
                task_id, req.owner, req.fencing_token, req.error_message,
                expected_version=req.expected_version,
            )
        except StateTransitionError as err:
            raise _error(err) from err
        return {
            "result": verdict.value,
            "task": _task_resp(task).model_dump() if task else None,
        }

    @router.get("/tasks", response_model=list[TaskResponse])
    def list_tasks(limit: int = 100):
        return [_task_resp(t) for t in service.list_tasks(limit)]

    @router.get("/tasks/{task_id}", response_model=TaskResponse)
    def get_task(task_id: str):
        try:
            return _task_resp(service.get_task(task_id))
        except StateTransitionError as err:
            raise _error(err) from err

    return router
