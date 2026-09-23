"""TaskStore：基于 SQLite 的任务持久化与原子状态流转。

并发模型
--------
- 单文件 SQLite（WAL 模式），无任何外部服务依赖；
- 写事务一律 ``BEGIN IMMEDIATE`` + 进程内锁串行化，跨进程/跨线程由 SQLite
  文件锁 + busy_timeout 兜底；
- 所有状态流转都用“带 WHERE 条件的单条 UPDATE”完成，条件包含旧状态/版本号/
  租约归属/fencing token/租约时间。条件不满足即影响 0 行并给出可区分原因，
  从根上杜绝读后写覆盖；
- 租约回收（过期 RUNNING -> PENDING -> 重新 RUNNING）在同一事务内完成，
  保证同一任务任意时刻只有一个消费者持有。
"""

from __future__ import annotations

import contextlib
import sqlite3
import threading
import uuid
from collections.abc import Iterable

from .clock import Clock
from .schema import SCHEMA_VERSION, migrate
from .types import (
    CompleteResult,
    EnqueueOutcome,
    EnqueueResult,
    Lease,
    LeaseResult,
    ReleaseResult,
    RenewResult,
    RetryResult,
    RejectReason,
    StateTransitionError,
    Task,
    TaskStatus,
)

_TASK_COLUMNS = (
    "id", "idempotency_key", "payload", "status", "available_at",
    "lease_owner", "lease_expires_at", "fencing_token", "attempt_count",
    "max_attempts", "backoff_base_seconds", "backoff_factor", "version",
    "result_payload", "error_message", "created_at", "updated_at",
)


def _row_to_task(row: sqlite3.Row) -> Task:
    return Task(
        id=row["id"],
        idempotency_key=row["idempotency_key"],
        payload=row["payload"],
        status=TaskStatus(row["status"]),
        available_at=row["available_at"],
        lease_owner=row["lease_owner"],
        lease_expires_at=row["lease_expires_at"],
        fencing_token=row["fencing_token"],
        attempt_count=row["attempt_count"],
        max_attempts=row["max_attempts"],
        backoff_base_seconds=row["backoff_base_seconds"],
        backoff_factor=row["backoff_factor"],
        version=row["version"],
        result_payload=row["result_payload"],
        error_message=row["error_message"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


class TaskStore:
    """任务存储与原子操作。"""

    def __init__(
        self,
        db_path: str,
        clock: Clock,
        *,
        default_max_attempts: int = 3,
        default_backoff_base: float = 1.0,
        default_backoff_factor: float = 2.0,
        default_lease_seconds: float = 30.0,
    ):
        self._db_path = db_path
        self._clock = clock
        self._default_max_attempts = int(default_max_attempts)
        self._default_backoff_base = float(default_backoff_base)
        self._default_backoff_factor = float(default_backoff_factor)
        self.default_lease_seconds = float(default_lease_seconds)
        self._lock = threading.RLock()
        # check_same_thread=False：服务可被多线程共享；写入由进程内锁串行。
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute("PRAGMA foreign_keys=ON")
        migrate(self._conn, now=self._clock.now())

    # ---- 维护 ----
    @property
    def schema_version(self) -> int:
        return SCHEMA_VERSION

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def __enter__(self) -> "TaskStore":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    @contextlib.contextmanager
    def _write_tx(self):
        """串行化的立即写事务：提交/回滚配对，杜绝读后写交叉。"""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield self._conn
            except Exception:
                self._conn.rollback()
                raise
            else:
                self._conn.commit()

    # ---- 读 ----
    def _select(self, conn: sqlite3.Connection, where: str, key: str) -> Task | None:
        cur = conn.execute(
            f"SELECT {', '.join(_TASK_COLUMNS)} FROM tasks WHERE {where}", (key,)
        )
        row = cur.fetchone()
        return _row_to_task(row) if row else None

    def get(self, task_id: str) -> Task:
        task = self._select(self._conn, "id = ?", task_id)
        if task is None:
            raise StateTransitionError(
                RejectReason.NOT_FOUND, f"任务不存在: {task_id}", task_id=task_id
            )
        return task

    def get_by_idempotency_key(self, key: str) -> Task | None:
        return self._select(self._conn, "idempotency_key = ?", key)

    def list_tasks(self, limit: int = 100) -> list[Task]:
        cur = self._conn.execute(
            f"SELECT {', '.join(_TASK_COLUMNS)} FROM tasks "
            "ORDER BY created_at, id LIMIT ?",
            (int(limit),),
        )
        return [_row_to_task(r) for r in cur.fetchall()]

    # ---- 写：入队 ----
    def enqueue(
        self,
        idempotency_key: str,
        payload: str = "",
        *,
        max_attempts: int | None = None,
        backoff_base_seconds: float | None = None,
        backoff_factor: float | None = None,
    ) -> EnqueueResult:
        if not isinstance(idempotency_key, str) or not idempotency_key:
            raise StateTransitionError(
                RejectReason.INVALID_PAYLOAD, "idempotency_key 不能为空"
            )
        max_attempts = (
            self._default_max_attempts if max_attempts is None else int(max_attempts)
        )
        backoff_base = (
            self._default_backoff_base
            if backoff_base_seconds is None
            else float(backoff_base_seconds)
        )
        backoff_factor = (
            self._default_backoff_factor
            if backoff_factor is None
            else float(backoff_factor)
        )
        if max_attempts < 1:
            raise StateTransitionError(
                RejectReason.INVALID_PAYLOAD, "max_attempts 必须 >= 1"
            )

        for _ in (1, 2):
            try:
                with self._write_tx() as conn:
                    existing = self._select(conn, "idempotency_key = ?", idempotency_key)
                    if existing is not None:
                        return self._converge(existing)
                    now = self._clock.now()
                    task_id = uuid.uuid4().hex
                    try:
                        conn.execute(
                            "INSERT INTO tasks(id, idempotency_key, payload, status, "
                            "available_at, fencing_token, attempt_count, max_attempts, "
                            "backoff_base_seconds, backoff_factor, version, "
                            "created_at, updated_at) "
                            "VALUES (?,?,?,?,?, 0, 0, ?,?,?, 1, ?,?)",
                            (
                                task_id, idempotency_key, payload,
                                TaskStatus.PENDING.value, now, max_attempts,
                                backoff_base, backoff_factor, now, now,
                            ),
                        )
                    except sqlite3.IntegrityError:
                        # 并发下唯一索引冲突：回滚本事务后重读收敛。
                        raise _RetryEnqueue from None
                    created = self._select(conn, "id = ?", task_id)
                    return EnqueueResult(EnqueueOutcome.CREATED, created)
            except _RetryEnqueue:
                # 并发下唯一索引冲突：重读并收敛（极少发生）。
                continue
        raise StateTransitionError(  # pragma: no cover - 理论不可达
            RejectReason.INVALID_PAYLOAD, "入队冲突重试失败"
        )

    @staticmethod
    def _converge(existing: Task) -> EnqueueResult:
        """重复提交的收敛规则：PENDING 去重；RUNNING/终态拒绝且原因可区分。"""
        if existing.status is TaskStatus.RUNNING:
            raise StateTransitionError(
                RejectReason.ALREADY_RUNNING,
                f"任务 {existing.id} 正在被 {existing.lease_owner} 执行，"
                "不得重复入队",
                task_id=existing.id,
            )
        if existing.status in (TaskStatus.SUCCEEDED, TaskStatus.FAILED):
            raise StateTransitionError(
                RejectReason.ALREADY_TERMINAL,
                f"任务 {existing.id} 已进入终态 {existing.status.value}，"
                "不得重复入队",
                task_id=existing.id,
            )
        # PENDING（含退避等待中）：收敛到同一条，不产生新记录。
        return EnqueueResult(EnqueueOutcome.DEDUPED_PENDING, existing)

    # ---- 写：领取 ----
    def claim_next(
        self,
        owner: str,
        *,
        lease_seconds: float | None = None,
        task_types: Iterable[str] | None = None,
    ) -> tuple[LeaseResult, Lease | None]:
        if not owner:
            raise StateTransitionError(
                RejectReason.INVALID_PAYLOAD, "owner 不能为空"
            )
        lease = (
            self.default_lease_seconds if lease_seconds is None else float(lease_seconds)
        )
        if lease <= 0:
            raise StateTransitionError(
                RejectReason.INVALID_PAYLOAD, "lease_seconds 必须 > 0"
            )
        now = self._clock.now()
        with self._write_tx() as conn:
            # 1) 先找出“租约已过期但仍是 RUNNING”的任务（回收候选）。
            #    COALESCE 兜底：兼容升级前遗留的 NULL 租期行（视为已过期）。
            expired_ids = [
                r[0]
                for r in conn.execute(
                    "SELECT id FROM tasks "
                    "WHERE status = 'RUNNING' "
                    "AND COALESCE(lease_expires_at, 0) <= ? "
                    "ORDER BY lease_expires_at, id",
                    (now,),
                ).fetchall()
            ]
            # 2) 原子回收：条件 UPDATE，仅过期 RUNNING 才能被改回 PENDING。
            if expired_ids:
                conn.execute(
                    "UPDATE tasks SET status = 'PENDING', lease_owner = NULL, "
                    "lease_expires_at = NULL, available_at = ?, "
                    "version = version + 1, updated_at = ? "
                    "WHERE status = 'RUNNING' "
                    "AND COALESCE(lease_expires_at, 0) <= ?",
                    (now, now, now),
                )
            # 3) 选一个可领取任务（PENDING 且退避窗口已过）。
            row = conn.execute(
                f"SELECT {', '.join(_TASK_COLUMNS)} FROM tasks "
                "WHERE status = 'PENDING' AND available_at <= ? "
                "ORDER BY available_at, created_at, id LIMIT 1",
                (now,),
            ).fetchone()
            if row is None:
                return LeaseResult.EMPTY, None
            task_id = row["id"]
            old_version = row["version"]
            new_token = int(row["fencing_token"]) + 1
            expires = now + lease
            # 4) 条件抢占：旧状态/版本必须仍匹配，保证只有一个消费者抢到。
            cur = conn.execute(
                "UPDATE tasks SET status = 'RUNNING', lease_owner = ?, "
                "lease_expires_at = ?, fencing_token = ?, "
                "attempt_count = attempt_count + 1, "
                "version = version + 1, updated_at = ? "
                "WHERE id = ? AND status = 'PENDING' AND available_at <= ? "
                "AND version = ?",
                (owner, expires, new_token, now, task_id, now, old_version),
            )
            if cur.rowcount != 1:  # pragma: no cover - 串行事务下不可达
                raise StateTransitionError(
                    RejectReason.VERSION_CONFLICT,
                    "领取抢占失败，任务已被其他消费者拿走",
                    task_id=task_id,
                )
            outcome = (
                LeaseResult.EXPIRED_REACQUIRED
                if task_id in expired_ids
                else LeaseResult.ACQUIRED
            )
            return outcome, Lease(
                task_id=task_id,
                owner=owner,
                fencing_token=new_token,
                acquired_at=now,
                expires_at=expires,
            )

    # ---- 持有者身份判定（可区分拒绝原因）----
    @staticmethod
    def _check_holdership(
        task: Task,
        owner: str,
        fencing_token: int,
        *,
        expired_reason: RejectReason,
    ) -> RejectReason | None:
        """返回 None 表示当前租约凭据有效；否则返回拒绝原因。"""
        if task.status is not TaskStatus.RUNNING:
            return RejectReason.NOT_RUNNING
        if task.lease_owner != owner or task.fencing_token != fencing_token:
            return RejectReason.LEASE_HELD_BY_OTHER
        if task.lease_expires_at is None:  # pragma: no cover - RUNNING 必有租期
            return RejectReason.NOT_RUNNING
        return None

    def renew(
        self,
        task_id: str,
        owner: str,
        fencing_token: int,
        *,
        lease_seconds: float | None = None,
    ) -> tuple[RenewResult, Lease | None]:
        lease = (
            self.default_lease_seconds if lease_seconds is None else float(lease_seconds)
        )
        if lease <= 0:
            raise StateTransitionError(
                RejectReason.INVALID_PAYLOAD, "lease_seconds 必须 > 0"
            )
        now = self._clock.now()
        with self._write_tx() as conn:
            task = self._select(conn, "id = ?", task_id)
            if task is None:
                raise StateTransitionError(
                    RejectReason.NOT_FOUND, f"任务不存在: {task_id}", task_id=task_id
                )
            reason = self._check_holdership(task, owner, fencing_token,
                                            expired_reason=RejectReason.LEASE_EXPIRED)
            if reason is RejectReason.NOT_RUNNING:
                return RenewResult.NOT_HELD, None
            if reason is RejectReason.LEASE_HELD_BY_OTHER:
                return RenewResult.HELD_BY_OTHER, None
            assert reason is None
            if now >= (task.lease_expires_at if task.lease_expires_at is not None else 0.0):
                return RenewResult.LEASE_EXPIRED, None
            expires = now + lease
            cur = conn.execute(
                "UPDATE tasks SET lease_expires_at = ?, version = version + 1, "
                "updated_at = ? WHERE id = ? AND status = 'RUNNING' "
                "AND lease_owner = ? AND fencing_token = ? AND version = ?",
                (expires, now, task_id, owner, fencing_token, task.version),
            )
            if cur.rowcount != 1:  # pragma: no cover - 串行事务下不可达
                conn.rollback()
                raise StateTransitionError(
                    RejectReason.VERSION_CONFLICT, "续期条件更新失败",
                    task_id=task_id,
                )
            return RenewResult.RENEWED, Lease(
                task_id=task_id, owner=owner, fencing_token=fencing_token,
                acquired_at=task.lease_expires_at and min(now, task.lease_expires_at),
                expires_at=expires,
            )

    def release(
        self,
        task_id: str,
        owner: str,
        fencing_token: int,
    ) -> ReleaseResult:
        now = self._clock.now()
        with self._write_tx() as conn:
            task = self._select(conn, "id = ?", task_id)
            if task is None:
                raise StateTransitionError(
                    RejectReason.NOT_FOUND, f"任务不存在: {task_id}", task_id=task_id
                )
            reason = self._check_holdership(task, owner, fencing_token,
                                            expired_reason=RejectReason.LEASE_EXPIRED)
            if reason is RejectReason.NOT_RUNNING:
                return ReleaseResult.NOT_HELD
            if reason is RejectReason.LEASE_HELD_BY_OTHER:
                return ReleaseResult.HELD_BY_OTHER
            assert reason is None
            if now >= (task.lease_expires_at if task.lease_expires_at is not None else 0.0):
                # 租约已过期：拒绝主动释放（归属将在下次 claim 时明确回收）。
                return ReleaseResult.LEASE_EXPIRED
            cur = conn.execute(
                "UPDATE tasks SET status = 'PENDING', lease_owner = NULL, "
                "lease_expires_at = NULL, available_at = ?, "
                "version = version + 1, updated_at = ? "
                "WHERE id = ? AND status = 'RUNNING' AND lease_owner = ? "
                "AND fencing_token = ? AND version = ?",
                (now, now, task_id, owner, fencing_token, task.version),
            )
            if cur.rowcount != 1:  # pragma: no cover - 串行事务下不可达
                raise StateTransitionError(
                    RejectReason.VERSION_CONFLICT, "释放条件更新失败",
                    task_id=task_id,
                )
            return ReleaseResult.RELEASED

    # ---- 写：结果提交 ----
    def _guard_submit(
        self,
        conn: sqlite3.Connection,
        task_id: str,
        owner: str,
        fencing_token: int,
        expected_version: int | None,
    ) -> Task:
        task = self._select(conn, "id = ?", task_id)
        if task is None:
            raise StateTransitionError(
                RejectReason.NOT_FOUND, f"任务不存在: {task_id}", task_id=task_id
            )
        if expected_version is not None and expected_version != task.version:
            raise StateTransitionError(
                RejectReason.VERSION_CONFLICT,
                f"版本过期：期望 {expected_version}，当前 {task.version}，"
                "基于过期版本的写入被拒绝",
                task_id=task_id,
            )
        return task

    def complete(
        self,
        task_id: str,
        owner: str,
        fencing_token: int,
        result_payload: str | None = None,
        *,
        expected_version: int | None = None,
    ) -> CompleteResult:
        now = self._clock.now()
        with self._write_tx() as conn:
            task = self._guard_submit(
                conn, task_id, owner, fencing_token, expected_version
            )
            if task.status is not TaskStatus.RUNNING:
                return CompleteResult.NOT_RUNNING
            if task.lease_owner != owner or task.fencing_token != fencing_token:
                return CompleteResult.HELD_BY_OTHER
            if now >= (task.lease_expires_at if task.lease_expires_at is not None else 0.0):
                # 持过期租约的消费者不得提交结果。
                return CompleteResult.LEASE_EXPIRED
            cur = conn.execute(
                "UPDATE tasks SET status = 'SUCCEEDED', lease_owner = NULL, "
                "lease_expires_at = NULL, available_at = ?, result_payload = ?, "
                "error_message = NULL, version = version + 1, updated_at = ? "
                "WHERE id = ? AND status = 'RUNNING' AND lease_owner = ? "
                "AND fencing_token = ? AND version = ?",
                (now, result_payload, now, task_id, owner, fencing_token,
                 task.version),
            )
            if cur.rowcount != 1:  # pragma: no cover - 串行事务下不可达
                raise StateTransitionError(
                    RejectReason.VERSION_CONFLICT, "完成条件更新失败",
                    task_id=task_id,
                )
            return CompleteResult.ACCEPTED

    def fail(
        self,
        task_id: str,
        owner: str,
        fencing_token: int,
        error_message: str | None = None,
        *,
        expected_version: int | None = None,
    ) -> tuple[RetryResult, Task | None]:
        now = self._clock.now()
        with self._write_tx() as conn:
            task = self._guard_submit(
                conn, task_id, owner, fencing_token, expected_version
            )
            if task.status is not TaskStatus.RUNNING:
                return RetryResult.NOT_RUNNING, None
            if task.lease_owner != owner or task.fencing_token != fencing_token:
                return RetryResult.HELD_BY_OTHER, None
            if now >= (task.lease_expires_at if task.lease_expires_at is not None else 0.0):
                return RetryResult.LEASE_EXPIRED, None

            exhausted = task.attempt_count >= task.max_attempts
            if exhausted:
                new_status = TaskStatus.FAILED.value
                available_at = now
            else:
                new_status = TaskStatus.PENDING.value
                delay = task.backoff_base_seconds * (
                    task.backoff_factor ** (task.attempt_count - 1)
                )
                available_at = now + delay
            cur = conn.execute(
                "UPDATE tasks SET status = ?, lease_owner = NULL, "
                "lease_expires_at = NULL, available_at = ?, error_message = ?, "
                "version = version + 1, updated_at = ? "
                "WHERE id = ? AND status = 'RUNNING' AND lease_owner = ? "
                "AND fencing_token = ? AND version = ?",
                (new_status, available_at, error_message, now, task_id,
                 owner, fencing_token, task.version),
            )
            if cur.rowcount != 1:  # pragma: no cover - 串行事务下不可达
                raise StateTransitionError(
                    RejectReason.VERSION_CONFLICT, "失败条件更新失败",
                    task_id=task_id,
                )
            updated = self._select(conn, "id = ?", task_id)
            if exhausted:
                return RetryResult.EXHAUSTED, updated
            return RetryResult.RETRY_SCHEDULED, updated


class _RetryEnqueue(Exception):
    """内部信号：唯一索引冲突后重读收敛。"""
