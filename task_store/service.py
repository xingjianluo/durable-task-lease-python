"""DurableTaskService：在 TaskStore 之上提供带判定日志的门面服务。

单测/运行日志会打印：
- 入队标识（idempotency_key）与创建/去重/拒绝结论；
- 租约信息（owner、fencing token、到期时间、剩余秒数）；
- 状态流转判定依据（旧状态、新状态、拒绝原因、版本号）。
"""

from __future__ import annotations

import logging

from .clock import Clock, SystemClock
from .store import TaskStore
from .types import (
    CompleteResult,
    EnqueueResult,
    Lease,
    LeaseResult,
    ReleaseResult,
    RenewResult,
    RetryResult,
    Task,
)

logger = logging.getLogger("task_store.service")


def _lease_desc(lease: Lease | None, now: float) -> str:
    if lease is None:
        return "<无租约>"
    return (
        f"task={lease.task_id} owner={lease.owner} fencing={lease.fencing_token} "
        f"acquired_at={lease.acquired_at:.3f} expires_at={lease.expires_at:.3f} "
        f"剩余={lease.expires_at - now:.3f}s"
    )


class DurableTaskService:
    """面向应用的门面：组合存储与时间源，输出可解释日志。"""

    def __init__(
        self,
        db_path: str = "tasks.db",
        clock: Clock | None = None,
        *,
        lease_seconds: float = 30.0,
        default_max_attempts: int = 3,
        backoff_base_seconds: float = 1.0,
        backoff_factor: float = 2.0,
    ):
        self._clock = clock or SystemClock()
        self._store = TaskStore(
            db_path,
            self._clock,
            default_max_attempts=default_max_attempts,
            default_backoff_base=backoff_base_seconds,
            default_backoff_factor=backoff_factor,
            default_lease_seconds=lease_seconds,
        )
        logger.info(
            "服务初始化 db=%s schema_version=%d lease=%ss max_attempts=%d",
            db_path, self._store.schema_version, lease_seconds, default_max_attempts,
        )

    @property
    def store(self) -> TaskStore:
        return self._store

    def close(self) -> None:
        self._store.close()

    def __enter__(self) -> "DurableTaskService":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def enqueue(self, idempotency_key: str, payload: str = "", **kwargs) -> EnqueueResult:
        result = self._store.enqueue(idempotency_key, payload, **kwargs)
        logger.info(
            "入队标识=%s 判定=%s task_id=%s status=%s version=%d",
            idempotency_key, result.outcome.value, result.task.id,
            result.task.status.value, result.task.version,
        )
        return result

    def claim(
        self, owner: str, *, lease_seconds: float | None = None
    ) -> tuple[LeaseResult, Lease | None]:
        outcome, lease = self._store.claim_next(owner, lease_seconds=lease_seconds)
        logger.info(
            "领取 owner=%s 判定=%s %s",
            owner, outcome.value, _lease_desc(lease, self._clock.now()),
        )
        return outcome, lease

    def renew(self, task_id: str, owner: str, fencing_token: int, **kw) -> tuple[RenewResult, Lease | None]:
        result, lease = self._store.renew(task_id, owner, fencing_token, **kw)
        logger.info(
            "续期 task=%s owner=%s fencing=%d 判定=%s %s",
            task_id, owner, fencing_token, result.value,
            _lease_desc(lease, self._clock.now()),
        )
        return result, lease

    def release(self, task_id: str, owner: str, fencing_token: int) -> ReleaseResult:
        result = self._store.release(task_id, owner, fencing_token)
        logger.info(
            "释放 task=%s owner=%s fencing=%d 判定=%s status=%s",
            task_id, owner, fencing_token, result.value,
            self._store.get(task_id).status.value,
        )
        return result

    def complete(self, task_id: str, owner: str, fencing_token: int, result: str | None = None, **kw) -> CompleteResult:
        verdict = self._store.complete(task_id, owner, fencing_token, result, **kw)
        status = self._store.get(task_id).status.value
        logger.info(
            "提交成功结果 task=%s owner=%s fencing=%d 判定=%s 当前状态=%s",
            task_id, owner, fencing_token, verdict.value, status,
        )
        return verdict

    def fail(self, task_id: str, owner: str, fencing_token: int, error: str | None = None, **kw) -> tuple[RetryResult, Task | None]:
        verdict, task = self._store.fail(task_id, owner, fencing_token, error, **kw)
        if task is not None:
            logger.info(
                "提交失败结果 task=%s owner=%s fencing=%d 判定=%s 当前状态=%s "
                "attempt=%d/%d available_at=%.3f 依据=%s",
                task_id, owner, fencing_token, verdict.value, task.status.value,
                task.attempt_count, task.max_attempts, task.available_at,
                error or "-",
            )
        else:
            logger.info(
                "提交失败结果 task=%s owner=%s fencing=%d 判定=%s（结果未被接受）",
                task_id, owner, fencing_token, verdict.value,
            )
        return verdict, task

    def get_task(self, task_id: str) -> Task:
        return self._store.get(task_id)

    def list_tasks(self, limit: int = 100) -> list[Task]:
        return self._store.list_tasks(limit)
