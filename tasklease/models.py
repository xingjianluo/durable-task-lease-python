"""任务领域模型与状态定义。"""
from __future__ import annotations

import dataclasses
import enum
from typing import Any


class TaskStatus(enum.Enum):
    """任务状态机。

    PENDING -> RUNNING -> SUCCEEDED / FAILED(终态，重试耗尽)
    RUNNING -> PENDING (可重试失败，退避后重新可领取)
    RUNNING -> DEAD_LETTER (租约信息异常，运维标记人工处理)
    """

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    DEAD_LETTER = "dead_letter"

    @classmethod
    def terminal_values(cls) -> frozenset[str]:
        return frozenset(
            {cls.SUCCEEDED.value, cls.FAILED.value, cls.DEAD_LETTER.value}
        )


# 归档范围：仅这两种“正常终结”的任务会被移入归档区。
ARCHIVABLE_TERMINAL_STATUSES = frozenset(
    {TaskStatus.SUCCEEDED, TaskStatus.FAILED}
)


@dataclasses.dataclass
class Task:
    """一条本地持久化任务。

    ``lease_token`` 只在“领取的当次返回值”上携带（等于本次执行凭证）；
    普通查询出于凭证最小暴露原则不会回填它（读到 None 不代表租约没有凭证，
    请以领取返回的 ``LeaseGrant.token`` 为准）。
    """

    id: str
    idempotency_key: str
    payload: dict[str, Any]
    status: TaskStatus
    attempt: int = 0
    max_attempts: int = 3
    version: int = 0
    lease_owner: str | None = None
    lease_expires_at: float | None = None
    lease_token: str | None = None
    available_at: float = 0.0
    last_error: str | None = None
    created_at: float = 0.0
    updated_at: float = 0.0
    archived: bool = False
    archived_at: float | None = None

    def is_terminal(self) -> bool:
        return self.status in (
            TaskStatus.SUCCEEDED,
            TaskStatus.FAILED,
            TaskStatus.DEAD_LETTER,
        )

    def is_archivable_terminal(self) -> bool:
        return self.status in ARCHIVABLE_TERMINAL_STATUSES

    def lease_active(self, now: float) -> bool:
        return (
            self.status is TaskStatus.RUNNING
            and self.lease_owner is not None
            and self.lease_expires_at is not None
            and self.lease_expires_at > now
        )


@dataclasses.dataclass(frozen=True)
class LeaseGrant:
    """一次领取成功的回执：任务快照 + 只属于本次执行的凭证。"""

    task: Task
    token: str

    @property
    def id(self) -> str:
        return self.task.id


@dataclasses.dataclass(frozen=True)
class TaskEvent:
    """一条执行历史事件（状态流转/租约轮换/运维动作的审计记录）。"""

    id: int
    task_id: str
    event: str
    created_at: float
    actor: str | None = None
    from_status: str | None = None
    to_status: str | None = None
    version: int = 0
    reason: str = ""
    lease_token: str | None = None
    archived: bool = False


@dataclasses.dataclass(frozen=True)
class Page:
    """分页结果。

    ``next_cursor`` 为 None 表示没有更多；否则作为下一次查询的 cursor 传入。
    """

    items: list[Any]
    next_cursor: str | None


@dataclasses.dataclass(frozen=True)
class ResultView:
    """任务终态结果读回视图：状态、产出、失败原因与尝试次数。"""

    task_id: str
    idempotency_key: str
    status: TaskStatus
    result: dict[str, Any]
    last_error: str | None
    attempt: int
    max_attempts: int
    created_at: float
    updated_at: float
    archived: bool
