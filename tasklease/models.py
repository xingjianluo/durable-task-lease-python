"""任务领域模型与状态定义。"""
from __future__ import annotations

import dataclasses
import enum
from typing import Any


class TaskStatus(enum.Enum):
    """任务状态机。

    PENDING -> RUNNING -> SUCCEEDED / FAILED(终态，重试耗尽)
    RUNNING -> PENDING (可重试失败，退避后重新可领取)
    RUNNING -> QUARANTINED (旧版本升级遗留的租约异常，等待人工处理)
    """

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    QUARANTINED = "quarantined"


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
    # 本次执行凭证：每次 acquire 重新发放，轮换后旧凭证立刻失效。
    lease_token: str | None = None
    # 成功产出（JSON 解码后），失败时为 None。
    result: dict[str, Any] | None = None
    # 非 None 表示该行已被搬到归档表（主表视图下恒为 None）。
    archived_at: float | None = None

    def is_terminal(self) -> bool:
        return self.status in (TaskStatus.SUCCEEDED, TaskStatus.FAILED)

    def lease_active(self, now: float) -> bool:
        return (
            self.status is TaskStatus.RUNNING
            and self.lease_owner is not None
            and self.lease_expires_at is not None
            and self.lease_expires_at > now
        )


@dataclasses.dataclass
class EventRecord:
    """task_events 审计流中的一条执行历史。"""

    id: int
    task_id: str
    event: str
    actor: str | None
    lease_token: str | None
    from_status: str | None
    to_status: str | None
    version: int
    reason: str | None
    created_at: float
    archived: bool = False


@dataclasses.dataclass
class Page:
    """分页查询结果。"""

    items: list[Any]
    limit: int
    next_cursor: int | None = None

