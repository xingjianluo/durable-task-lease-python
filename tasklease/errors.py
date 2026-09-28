"""错误类型定义：非法操作以可区分的拒绝原因返回。"""
from __future__ import annotations

import dataclasses
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
    TOKEN_MISSING = "token_missing"
    TOKEN_MISMATCH = "token_mismatch"
    TASK_QUARANTINED = "task_quarantined"


class ReleaseOutcome(enum.Enum):
    """释放租约的结果（四类可区分）。"""

    RELEASED = "released"
    NOT_HELD = "not_held"
    ALREADY_EXPIRED = "already_expired"
    TOKEN_MISMATCH = "token_mismatch"


class HeartbeatOutcome(enum.Enum):
    """租约续期的结果（四类可区分）。"""

    RENEWED = "renewed"
    NOT_HELD = "not_held"
    EXPIRED = "expired"
    TOKEN_MISMATCH = "token_mismatch"


class EnqueueOutcome(enum.Enum):
    """入队结果：新建或幂等命中既有任务。"""

    CREATED = "created"
    EXISTING = "existing"


class RecoveryState(enum.Enum):
    """RUNNING 任务租约健康度的分类结论。"""

    ACTIVE = "active"          # 正在被有效持有，不可碰
    EXPIRED = "expired"        # 租约已过期，可安全恢复为可领取
    CORRUPT = "corrupt"        # 租约信息不完整/缺失，需恢复或人工处理


class RecoveryAction(enum.Enum):
    """扫描/恢复动作对单条任务的处理结论。"""

    SKIPPED_ACTIVE = "skipped_active"
    REQUEUED_EXPIRED = "requeued_expired"
    EXPIRED_ALREADY_RECOVERED = "expired_already_recovered"
    QUARANTINED_CORRUPT = "quarantined_corrupt"
    REQUEUED_CORRUPT = "requeued_corrupt"
    CORRUPT_ALREADY_QUARANTINED = "corrupt_already_quarantined"
    CORRUPT_ALREADY_RECOVERED = "corrupt_already_recovered"
    NOT_RUNNING = "not_running"


@dataclasses.dataclass
class RecoveryReport:
    """一次异常运行任务扫描/恢复的汇总报告（可重复执行的结果快照）。"""

    scanned_running: int = 0
    active: int = 0
    expired: int = 0
    corrupt: int = 0
    recovered: int = 0
    quarantined: int = 0
    skipped: int = 0
    actions: list[tuple[str, RecoveryAction, str]] = dataclasses.field(
        default_factory=list
    )  # (task_id, action, detail)


class ArchiveOutcome(enum.Enum):
    """单条终态任务归档动作的结论。"""

    ARCHIVED = "archived"
    ALREADY_ARCHIVED = "already_archived"
    SKIPPED_NON_TERMINAL = "skipped_non_terminal"
    NOT_FOUND = "not_found"


@dataclasses.dataclass
class ArchiveReport:
    """一次归档批处理的汇总报告。"""

    archived: int = 0
    already_archived: int = 0
    skipped_non_terminal: int = 0
    actions: list[tuple[str, ArchiveOutcome]] = dataclasses.field(default_factory=list)


class TaskRejected(Exception):
    """带可区分原因的任务操作拒绝异常。"""

    def __init__(self, reason: RejectReason, message: str = "") -> None:
        self.reason = reason
        super().__init__(message or reason.value)
