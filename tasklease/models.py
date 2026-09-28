"""任务领域模型与状态定义。"""
from __future__ import annotations

import dataclasses
import enum
from typing import Any


class TaskStatus(enum.Enum):
    """任务状态机。

    PENDING -> RUNNING -> SUCCEEDED / FAILED(终态，重试耗尽)
    RUNNING -> PENDING (可重试失败，退避后重新可领取)
    RUNNING(租约信息异常) -> QUARANTINED (需人工处理，可再人工放回 PENDING)
    SUCCEEDED / FAILED -> (归档) 记录移入 tasks_archive，状态保持不变
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
    lease_token: str | None = None
    available_at: float = 0.0
    last_error: str | None = None
    created_at: float = 0.0
    updated_at: float = 0.0
    archived: bool = False

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
class LeaseGrant(Task):
    """一次成功领取的授权凭证（同时也是这次领取视角下的 Task 快照）。

    继承自 ``Task`` 以便老调用方直接把它当任务用；新增的 ``lease_token``
    只对本次租约世代有效：租约过期后被他人（或同名消费者）重新领取时，
    存储层会签发新 token，旧 token 立即作废，不能再提交结果、上报失败、
    续期或释放。需要纯 Task 视图时使用 ``.as_task()``。
    """

    lease_token: str = ""

    def as_task(self) -> Task:
        """返回不含凭证语义的纯 Task 副本。"""
        return Task(
            id=self.id,
            idempotency_key=self.idempotency_key,
            payload=self.payload,
            status=self.status,
            attempt=self.attempt,
            max_attempts=self.max_attempts,
            version=self.version,
            lease_owner=self.lease_owner,
            lease_expires_at=self.lease_expires_at,
            lease_token=self.lease_token,
            available_at=self.available_at,
            last_error=self.last_error,
            created_at=self.created_at,
            updated_at=self.updated_at,
            archived=self.archived,
        )

    @property
    def task(self) -> Task:
        return self.as_task()

    @property
    def task_id(self) -> str:
        return self.id
