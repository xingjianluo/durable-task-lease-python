"""任务领域模型与状态定义。"""
from __future__ import annotations

import dataclasses
import enum
from typing import Any


class TaskStatus(enum.Enum):
    """任务状态机。

    PENDING -> RUNNING -> SUCCEEDED / FAILED(终态，重试耗尽)
    RUNNING -> PENDING (可重试失败，退避后重新可领取)
    """

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


@dataclasses.dataclass
class Task:
    """一条本地持久化任务。"""

    id: str
    idempotency_key: str
    payload: dict[str, Any]
    status: TaskStatus
    attempt: int = 0
    max_attempts: int = 3
    version: int = 0
    lease_owner: str | None = None
    lease_expires_at: float | None = None
    available_at: float = 0.0
    last_error: str | None = None
    created_at: float = 0.0
    updated_at: float = 0.0

    def is_terminal(self) -> bool:
        return self.status in (TaskStatus.SUCCEEDED, TaskStatus.FAILED)

    def lease_active(self, now: float) -> bool:
        return (
            self.status is TaskStatus.RUNNING
            and self.lease_owner is not None
            and self.lease_expires_at is not None
            and self.lease_expires_at > now
        )
