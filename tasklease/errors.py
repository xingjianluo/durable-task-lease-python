"""错误类型定义：非法操作以可区分的拒绝原因返回。"""
from __future__ import annotations

import enum


class RejectReason(enum.Enum):
    """任务操作被拒绝的可区分原因。"""

    TASK_EXECUTING = "task_executing"
    TASK_TERMINAL = "task_terminal"
    LEASE_HELD_BY_OTHER = "lease_held_by_other"
    LEASE_EXPIRED = "lease_expired"
    VERSION_CONFLICT = "version_conflict"
    TASK_NOT_FOUND = "task_not_found"
    NOT_PENDING_OR_UNCLAIMED = "not_pending_or_unclaimed"


class ReleaseOutcome(enum.Enum):
    """释放租约的结果（三类可区分）。"""

    RELEASED = "released"
    NOT_HELD = "not_held"
    ALREADY_EXPIRED = "already_expired"


class HeartbeatOutcome(enum.Enum):
    """租约续期的结果（三类可区分）。"""

    RENEWED = "renewed"
    NOT_HELD = "not_held"
    EXPIRED = "expired"


class EnqueueOutcome(enum.Enum):
    """入队结果：新建或幂等命中既有任务。"""

    CREATED = "created"
    EXISTING = "existing"


class TaskRejected(Exception):
    """带可区分原因的任务操作拒绝异常。"""

    def __init__(self, reason: RejectReason, message: str = "") -> None:
        self.reason = reason
        super().__init__(message or reason.value)
