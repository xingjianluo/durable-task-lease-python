"""HTTP 请求/响应模型。"""

from __future__ import annotations

from pydantic import BaseModel, Field


class EnqueueRequest(BaseModel):
    idempotency_key: str = Field(..., min_length=1)
    payload: str = ""
    max_attempts: int | None = Field(default=None, ge=1)
    backoff_base_seconds: float | None = Field(default=None, gt=0)
    backoff_factor: float | None = Field(default=None, ge=1)


class ClaimRequest(BaseModel):
    owner: str = Field(..., min_length=1)
    lease_seconds: float | None = Field(default=None, gt=0)


class LeaseHolder(BaseModel):
    task_id: str
    owner: str
    fencing_token: int
    acquired_at: float
    expires_at: float


class ClaimResponse(BaseModel):
    outcome: str
    lease: LeaseHolder | None = None
    task_id: str | None = None
    payload: str | None = None


class LeaseActionRequest(BaseModel):
    owner: str = Field(..., min_length=1)
    fencing_token: int
    lease_seconds: float | None = Field(default=None, gt=0)


class ReleaseResponse(BaseModel):
    result: str


class RenewResponse(BaseModel):
    result: str
    lease: LeaseHolder | None = None


class ResultRequest(BaseModel):
    owner: str = Field(..., min_length=1)
    fencing_token: int
    result_payload: str | None = None
    expected_version: int | None = None


class FailRequest(BaseModel):
    owner: str = Field(..., min_length=1)
    fencing_token: int
    error_message: str | None = None
    expected_version: int | None = None


class TaskResponse(BaseModel):
    id: str
    idempotency_key: str
    payload: str
    status: str
    available_at: float
    lease_owner: str | None
    lease_expires_at: float | None
    fencing_token: int
    attempt_count: int
    max_attempts: int
    version: int
    result_payload: str | None
    error_message: str | None
    created_at: float
    updated_at: float
