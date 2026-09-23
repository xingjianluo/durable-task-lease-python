"""本地持久化任务领取/执行存储层。

不依赖任何外部服务（使用内置 sqlite3），支持：
- 幂等入队与状态机约束
- 并发领取与租约（lease）管理
- 失败重试（上限 + 退避）与乐观版本并发控制
"""

from .types import (
    EnqueueOutcome,
    EnqueueResult,
    Lease,
    LeaseResult,
    ReleaseResult,
    RenewResult,
    RejectReason,
    StateTransitionError,
    Task,
    TaskStatus,
)

__all__ = [
    "EnqueueOutcome",
    "EnqueueResult",
    "Lease",
    "LeaseResult",
    "ReleaseResult",
    "RenewResult",
    "RejectReason",
    "StateTransitionError",
    "Task",
    "TaskStatus",
]
