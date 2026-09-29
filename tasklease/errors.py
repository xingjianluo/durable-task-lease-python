"""错误类型定义：非法操作以可区分的拒绝原因返回。"""
from __future__ import annotations

import enum


class RejectReason(enum.Enum):
    """任务操作被拒绝的可区分原因。"""

    TASK_EXECUTING = "task_executing"
    TASK_TERMINAL = "task_terminal"
    LEASE_HELD_BY_OTHER = "lease_held_by_other"
    LEASE_EXPIRED = "lease_expired"
    LEASE_TOKEN_MISMATCH = "lease_token_mismatch"
    LEASE_MALFORMED = "lease_malformed"
    VERSION_CONFLICT = "version_conflict"
    TASK_NOT_FOUND = "task_not_found"
    NOT_PENDING_OR_UNCLAIMED = "not_pending_or_unclaimed"
    TASK_DEAD_LETTER = "task_dead_letter"


class ReleaseOutcome(enum.Enum):
    """释放租约的结果（四类可区分）。"""

    RELEASED = "released"
    NOT_HELD = "not_held"
    ALREADY_EXPIRED = "already_expired"
    INVALID_TOKEN = "invalid_token"


class HeartbeatOutcome(enum.Enum):
    """租约续期的结果（四类可区分）。"""

    RENEWED = "renewed"
    NOT_HELD = "not_held"
    EXPIRED = "expired"
    INVALID_TOKEN = "invalid_token"


class EnqueueOutcome(enum.Enum):
    """入队结果：新建或幂等命中既有任务。"""

    CREATED = "created"
    EXISTING = "existing"


class LeaseHealth(enum.Enum):
    """运行中任务的租约健康分类（运维判定用）。

    HELD       —— 仍被有效持有（owner/token 齐全且租约未过期），不得恢复；
    EXPIRED    —— 租约字段齐全但已过期，可以安全恢复为可领取；
    MALFORMED  —— 租约信息异常（owner/到期时间缺失或矛盾），需要恢复；
    NOT_RUNNING—— 任务并非运行中，不属于异常运行任务的处理范围。
    """

    HELD = "held"
    EXPIRED = "expired"
    MALFORMED = "malformed"
    NOT_RUNNING = "not_running"


class RecoveryAction(enum.Enum):
    """异常运行任务的恢复策略。

    REQUEUE —— 恢复为 PENDING，立即可被重新领取（attempt 不额外增加）；
    MANUAL  —— 标记为 DEAD_LETTER，等待人工处理，不再被自动领取。
    """

    REQUEUE = "requeue"
    MANUAL = "manual"


class RecoveryResult(enum.Enum):
    """单条异常运行任务执行恢复后的结果。"""

    REQUEUED = "requeued"
    MARKED_MANUAL = "marked_manual"
    SKIPPED_HELD = "skipped_held"
    SKIPPED_NOT_RUNNING = "skipped_not_running"


class TaskRejected(Exception):
    """带可区分原因的任务操作拒绝异常。"""

    def __init__(self, reason: RejectReason, message: str = "") -> None:
        self.reason = reason
        super().__init__(message or reason.value)
