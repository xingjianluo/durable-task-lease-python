"""领域类型定义：任务状态机、可区分的拒绝原因、租约与操作结果。

状态机
------
    PENDING ──claim──▶ RUNNING ──complete──▶ SUCCEEDED(终态)
       ▲                │
       │                ├──fail(retry<上限)──▶ PENDING（等待退避窗口，等待期间不可领取）
       │                ├──fail(retry>=上限)──▶ FAILED(终态)
       │                ├──release──────────▶ PENDING
       │                └──租约过期──────────▶ PENDING（领取时回收）
       └──────────────────────────────────────

状态判定规则：
- 任务在 RUNNING 但 lease_expires_at <= 当前时间时，视为“租约已过期”，
  下一次 claim 会以单条 UPDATE 的条件写入方式把它原子回收为 PENDING 后重新领取。
- PENDING 且 available_at > 当前时间时，任务处于退避等待中，不会被领取。
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field


class TaskStatus(str, enum.Enum):
    """任务状态。"""

    PENDING = "PENDING"
    """待领取（也包含失败后等待退避窗口结束的任务，由 available_at 区分）。"""

    RUNNING = "RUNNING"
    """已被某个消费者领取并持有有效租约。"""

    SUCCEEDED = "SUCCEEDED"
    """终态：执行成功。"""

    FAILED = "FAILED"
    """终态：失败次数达到上限，不再重试。"""


TERMINAL_STATUSES = frozenset({TaskStatus.SUCCEEDED, TaskStatus.FAILED})


class RejectReason(str, enum.Enum):
    """可区分的拒绝原因（非法状态流转 / 重复入队等）。"""

    ALREADY_RUNNING = "ALREADY_RUNNING"
    """幂等键已存在且任务正在执行，禁止重复入队。"""

    ALREADY_TERMINAL = "ALREADY_TERMINAL"
    """幂等键已存在且任务已进入终态，禁止重复入队。"""

    NOT_RUNNING = "NOT_RUNNING"
    """目标任务不在 RUNNING 状态（已回收/从未领取/已终态）。"""

    LEASE_EXPIRED = "LEASE_EXPIRED"
    """消费者持有的租约已过期，执行权已不属于它。"""

    LEASE_HELD_BY_OTHER = "LEASE_HELD_BY_OTHER"
    """租约仍有效，但持有者是别的消费者。"""

    LEASE_STILL_VALID = "LEASE_STILL_VALID"
    """对一个仍然有效的租约执行“回收”类操作。"""

    VERSION_CONFLICT = "VERSION_CONFLICT"
    """基于过期版本的写入被拒绝（乐观锁）。"""

    NOT_FOUND = "NOT_FOUND"
    """任务不存在。"""

    INVALID_PAYLOAD = "INVALID_PAYLOAD"
    """入参非法（如空幂等键）。"""


class StateTransitionError(Exception):
    """所有被拒绝的状态流转统一抛出该异常，携带可区分的 reason。"""

    def __init__(self, reason: RejectReason, message: str, *, task_id: str | None = None):
        super().__init__(message)
        self.reason = reason
        self.message = message
        self.task_id = task_id

    def __str__(self) -> str:  # pragma: no cover - 调试辅助
        suffix = f" task={self.task_id}" if self.task_id else ""
        return f"[{self.reason.value}]{suffix} {self.message}"


@dataclass(frozen=True, slots=True)
class Lease:
    """消费者持有的租约凭据。"""

    task_id: str
    owner: str
    """持有者标识（消费者 ID）。"""

    fencing_token: int
    """单调递增的 fencing token：每次领取得到新值，执行权归属以它为准。"""

    acquired_at: float
    expires_at: float

    def is_expired(self, now: float) -> bool:
        return now >= self.expires_at


@dataclass(frozen=True, slots=True)
class Task:
    """一条任务记录的完整快照。"""

    id: str
    idempotency_key: str
    payload: str
    status: TaskStatus
    available_at: float
    """可领取时间（退避结束时间）；PENDING 且 now < available_at 时不可领取。"""

    lease_owner: str | None
    lease_expires_at: float | None
    fencing_token: int
    attempt_count: int
    """已领取次数（每被领取一次 +1）。"""

    max_attempts: int
    backoff_base_seconds: float
    backoff_factor: float
    version: int
    """乐观并发版本号，每次写入 +1；条件更新必须携带它。"""

    created_at: float
    updated_at: float
    result_payload: str | None = field(default=None)
    error_message: str | None = field(default=None)


class EnqueueOutcome(str, enum.Enum):
    CREATED = "CREATED"
    """新任务创建。"""

    DEDUPED_PENDING = "DEDUPED_PENDING"
    """幂等命中：任务已存在且仍在 PENDING，收敛到同一条，不产生重复记录。"""


class LeaseResult(str, enum.Enum):
    """领取（claim）结果。"""

    ACQUIRED = "ACQUIRED"
    """成功领取。"""

    EXPIRED_REACQUIRED = "EXPIRED_REACQUIRED"
    """旧租约已过期，被本消费者回收并重新领取（执行权明确转移）。"""

    EMPTY = "EMPTY"
    """当前没有可领取的任务。"""


class ReleaseResult(str, enum.Enum):
    """主动释放（release）结果。"""

    RELEASED = "RELEASED"
    """已主动释放，任务回到 PENDING。"""

    NOT_HELD = "NOT_HELD"
    """任务不在 RUNNING（从未被持有/已完成）。"""

    LEASE_EXPIRED = "LEASE_EXPIRED"
    """持有的租约已过期，拒绝释放。"""

    HELD_BY_OTHER = "HELD_BY_OTHER"
    """租约有效但属于别的消费者。"""


class RenewResult(str, enum.Enum):
    """租约续期（renew）结果。"""

    RENEWED = "RENEWED"
    """续期成功。"""

    NOT_HELD = "NOT_HELD"
    """任务不在 RUNNING。"""

    LEASE_EXPIRED = "LEASE_EXPIRED"
    """租约已过期，不能续期（只能重新领取）。"""

    HELD_BY_OTHER = "HELD_BY_OTHER"
    """租约属于别的消费者。"""


class CompleteResult(str, enum.Enum):
    """结果提交（complete / fail）结果。"""

    ACCEPTED = "ACCEPTED"
    """结果被接受。"""

    NOT_RUNNING = "NOT_RUNNING"
    """任务已不在 RUNNING。"""

    LEASE_EXPIRED = "LEASE_EXPIRED"
    """提交者持有过期租约，结果被拒绝。"""

    HELD_BY_OTHER = "HELD_BY_OTHER"
    """当前执行权属于更新的租约（fencing token 更新）。"""


class RetryResult(str, enum.Enum):
    """失败（fail）结果。"""

    RETRY_SCHEDULED = "RETRY_SCHEDULED"
    """未超上限：按退避重新排期，之后可被领取。"""

    EXHAUSTED = "EXHAUSTED"
    """达到上限：任务进入 FAILED 终态。"""

    NOT_RUNNING = "NOT_RUNNING"
    LEASE_EXPIRED = "LEASE_EXPIRED"
    HELD_BY_OTHER = "HELD_BY_OTHER"


@dataclass(frozen=True, slots=True)
class EnqueueResult:
    outcome: EnqueueOutcome
    task: Task
