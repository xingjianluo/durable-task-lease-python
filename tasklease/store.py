"""任务持久化存储：并发领取、租约、失败重试与乐观版本控制。

并发模型：进程内以一把可重入锁串行化会话访问；跨进程依赖 SQLite 的
写事务串行化与"条件 UPDATE ... WHERE"原子领取，保证同一任务任意时刻
只被一个消费者持有。所有状态变更都带版本号递增，过期版本写入被拒绝。
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from collections.abc import Callable
from typing import Any

from . import schema
from .errors import (
    EnqueueOutcome,
    HeartbeatOutcome,
    RejectReason,
    ReleaseOutcome,
    TaskRejected,
)
from .models import LeaseGrant, Task, TaskStatus

_PAYLOAD_COLUMNS = (
    "id",
    "idempotency_key",
    "payload",
    "status",
    "attempt",
    "max_attempts",
    "version",
    "lease_owner",
    "lease_expires_at",
    "lease_token",
    "available_at",
    "last_error",
    "result",
    "created_at",
    "updated_at",
)


def _short_token(token: str | None) -> str:
    """日志/事件里只记录凭证前缀，便于区分租约世代又不暴露完整凭证。"""
    if not token:
        return "-"
    return token[:8]


class TaskStore:
    """基于 SQLite 的本地任务存储。"""

    def __init__(
        self,
        db_path: str = ":memory:",
        default_lease_seconds: float = 30.0,
        backoff_base_seconds: float = 1.0,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._db_path = db_path
        self.default_lease_seconds = default_lease_seconds
        self.backoff_base_seconds = backoff_base_seconds
        self._clock = clock or time.time
        # check_same_thread=False + 自管锁，供多消费者线程共享同一存储。
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._lock = threading.RLock()
        schema.initialize(self._conn)

    # ---------- 基础工具 ----------

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def __enter__(self) -> TaskStore:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _now(self) -> float:
        return self._clock()

    def _to_task(self, row: sqlite3.Row, archived: bool = False) -> Task:
        data = dict(row)
        data["status"] = TaskStatus(data["status"])
        data["payload"] = json.loads(data.get("payload") or "{}")
        data.pop("result", None)
        data.pop("archived_at", None)
        data["archived"] = archived
        return Task(**data)

    def _fetch_by_id(
        self, task_id: str, *, include_archive: bool = True
    ) -> sqlite3.Row | None:
        cur = self._conn.execute(
            f"SELECT {', '.join(_PAYLOAD_COLUMNS)} FROM tasks WHERE id=?",
            (task_id,),
        )
        row = cur.fetchone()
        if row is not None or not include_archive:
            return row
        cur = self._conn.execute(
            f"SELECT {', '.join(_PAYLOAD_COLUMNS)}, archived_at "
            "FROM tasks_archive WHERE id=?",
            (task_id,),
        )
        return cur.fetchone()

    def _is_archived(self, task_id: str) -> bool:
        cur = self._conn.execute(
            "SELECT 1 FROM tasks_archive WHERE id=?", (task_id,)
        )
        return cur.fetchone() is not None

    def _fetch_by_key(self, key: str) -> sqlite3.Row | None:
        cur = self._conn.execute(
            f"SELECT {', '.join(_PAYLOAD_COLUMNS)} FROM tasks "
            "WHERE idempotency_key=?",
            (key,),
        )
        row = cur.fetchone()
        if row is not None:
            return row
        cur = self._conn.execute(
            f"SELECT {', '.join(_PAYLOAD_COLUMNS)}, archived_at "
            "FROM tasks_archive WHERE idempotency_key=?",
            (key,),
        )
        return cur.fetchone()

    def _log_event(
        self,
        task_id: str,
        event: str,
        actor: str | None,
        from_status: TaskStatus | None,
        to_status: TaskStatus | None,
        version: int,
        reason: str = "",
    ) -> None:
        self._conn.execute(
            "INSERT INTO task_events (task_id, event, actor, from_status, to_status, "
            "version, reason, created_at) VALUES (?,?,?,?,?,?,?,?)",
            (
                task_id,
                event,
                actor,
                from_status.value if from_status else None,
                to_status.value if to_status else None,
                version,
                reason,
                self._now(),
            ),
        )

    # ---------- 查询 ----------

    def get(self, task_id: str) -> Task:
        with self._lock:
            row = self._fetch_by_id(task_id)
            if row is None:
                raise TaskRejected(
                    RejectReason.TASK_NOT_FOUND, f"task {task_id!r} 不存在"
                )
            archived = "archived_at" in row.keys()  # noqa: SIM118  # sqlite3.Row 不是 dict
            return self._to_task(row, archived=archived)

    def get_by_idempotency_key(self, key: str) -> Task | None:
        with self._lock:
            row = self._fetch_by_key(key)
            if row is None:
                return None
            archived = "archived_at" in row.keys()  # noqa: SIM118  # sqlite3.Row 不是 dict
            return self._to_task(row, archived=archived)

    # ---------- 幂等入队 ----------

    def enqueue(
        self,
        idempotency_key: str,
        payload: dict[str, Any] | None = None,
        max_attempts: int = 3,
    ) -> tuple[Task, EnqueueOutcome]:
        if max_attempts < 1:
            raise ValueError("max_attempts 必须 >= 1")
        now = self._now()
        with self._lock:
            return self._enqueue_locked(idempotency_key, payload or {}, max_attempts, now)

    def _enqueue_locked(
        self,
        key: str,
        payload: dict[str, Any],
        max_attempts: int,
        now: float,
    ) -> tuple[Task, EnqueueOutcome]:
        existing = self._fetch_by_key(key)
        if existing is not None:
            self._reject_if_enqueue_blocked(existing, now, key)
            self._log_event(
                existing["id"], "enqueue.idempotent_hit", None,
                TaskStatus(existing["status"]), TaskStatus(existing["status"]),
                existing["version"], f"幂等命中 key={key}",
            )
            self._conn.commit()
            archived = "archived_at" in existing.keys()  # noqa: SIM118
            return self._to_task(existing, archived=archived), EnqueueOutcome.EXISTING

        task_id = uuid.uuid4().hex
        body = json.dumps(payload, separators=(",", ":"), sort_keys=True)
        try:
            self._conn.execute(
                "INSERT INTO tasks (id, idempotency_key, payload, status, attempt, "
                "max_attempts, version, lease_owner, lease_expires_at, lease_token, "
                "available_at, last_error, result, created_at, updated_at) "
                "VALUES (?,?,?,'pending',0,?,0,NULL,NULL,NULL,?,NULL,NULL,?,?)",
                (task_id, key, body, max_attempts, now, now, now),
            )
            self._log_event(
                task_id, "enqueue.created", None, None, TaskStatus.PENDING, 0,
                f"新建任务 key={key}",
            )
            self._conn.commit()
        except sqlite3.IntegrityError:
            # 并发下唯一索引拦截：收敛到既有任务，按其当前状态判定。
            self._conn.rollback()
            existing = self._fetch_by_key(key)
            assert existing is not None
            self._reject_if_enqueue_blocked(existing, now, key)
            archived = "archived_at" in existing.keys()  # noqa: SIM118
            return self._to_task(existing, archived=archived), EnqueueOutcome.EXISTING

        row = self._fetch_by_id(task_id)
        assert row is not None
        return self._to_task(row), EnqueueOutcome.CREATED

    @staticmethod
    def _reject_if_enqueue_blocked(
        row: sqlite3.Row, now: float, key: str
    ) -> None:
        status = TaskStatus(row["status"])
        if status in (TaskStatus.SUCCEEDED, TaskStatus.FAILED):
            raise TaskRejected(
                RejectReason.TASK_TERMINAL,
                f"key={key} 的任务已处于终态 {status.value}，不得重复入队",
            )
        if status is TaskStatus.RUNNING:
            expires = row["lease_expires_at"]
            if expires is not None and expires > now and row["lease_owner"]:
                raise TaskRejected(
                    RejectReason.TASK_EXECUTING,
                    f"key={key} 的任务正在被 {row['lease_owner']} 执行，"
                    f"租约到期时间={expires}（当前 {now}），不得重复入队",
                )
            # RUNNING 但租约已过期：任务将被重新领取，重复提交直接收敛到同一任务。

    # ---------- 并发领取 ----------

    def acquire(
        self, consumer: str, lease_seconds: float | None = None
    ) -> LeaseGrant | None:
        if not consumer:
            raise ValueError("consumer 不能为空")
        ttl = lease_seconds if lease_seconds is not None else self.default_lease_seconds
        if ttl <= 0:
            raise ValueError("lease_seconds 必须 > 0")
        now = self._now()
        expires = now + ttl
        with self._lock:
            # 每次领取都为本次租约世代签发全新不透明凭证；
            # 旧世代（即使同名消费者）的凭证立即作废。
            token = uuid.uuid4().hex
            cur = self._conn.execute(
                """
                UPDATE tasks SET
                    status='running',
                    lease_owner=:owner,
                    lease_expires_at=:exp,
                    lease_token=:token,
                    attempt=attempt+1,
                    version=version+1,
                    updated_at=:now
                WHERE rowid = (
                    SELECT rowid FROM tasks
                    WHERE (status='pending' AND available_at<=:now)
                       OR (status='running'
                           AND lease_owner IS NOT NULL
                           AND lease_expires_at IS NOT NULL
                           AND lease_token IS NOT NULL
                           AND lease_expires_at<=:now)
                    ORDER BY available_at, created_at, id
                    LIMIT 1
                )
                RETURNING id, version
                """,
                {"owner": consumer, "exp": expires, "token": token, "now": now},
            )
            claimed = cur.fetchone()
            if claimed is None:
                self._conn.commit()
                return None
            task_id, new_version = claimed["id"], claimed["version"]
            self._log_event(
                task_id, "acquire", consumer, TaskStatus.RUNNING, TaskStatus.RUNNING,
                new_version,
                f"领取成功 lease_owner={consumer} lease_expires_at={expires} "
                f"ttl={ttl}s token={_short_token(token)}",
            )
            self._conn.commit()
            row = self._fetch_by_id(task_id, include_archive=False)
            assert row is not None
            kwargs = self._task_kwargs(row)
            kwargs["lease_token"] = token
            return LeaseGrant(**kwargs)

    @staticmethod
    def _task_kwargs(row: sqlite3.Row) -> dict[str, Any]:
        data = dict(row)
        data["status"] = TaskStatus(data["status"])
        data["payload"] = json.loads(data.get("payload") or "{}")
        data.pop("result", None)
        data.pop("archived_at", None)
        data.pop("archived", None)
        return data

    # ---------- 结果提交 ----------

    def _guard_lease(
        self,
        row: sqlite3.Row,
        consumer: str,
        now: float,
        expected_version: int | None,
        lease_token: str | None = None,
    ) -> TaskStatus:
        """校验状态机位置、租约归属与有效期、执行凭证、客户端版本。

        判定顺序即“执行权丧失”的解释顺序：
        终态 -> 未领取 -> 隔离中 -> 持有者不符 -> 租约过期
        -> 凭证缺失/作废 -> 版本冲突。
        """
        status = TaskStatus(row["status"])
        if status in (TaskStatus.SUCCEEDED, TaskStatus.FAILED):
            raise TaskRejected(
                RejectReason.TASK_TERMINAL,
                f"任务 {row['id']} 已处于终态 {status.value}，状态流转被拒绝",
            )
        if status is TaskStatus.QUARANTINED:
            raise TaskRejected(
                RejectReason.TASK_QUARANTINED,
                f"任务 {row['id']} 因租约信息异常已被隔离（quarantined），"
                f"需人工处理后才能继续执行",
            )
        if status is not TaskStatus.RUNNING:
            raise TaskRejected(
                RejectReason.NOT_PENDING_OR_UNCLAIMED,
                f"任务 {row['id']} 当前为 {status.value}，尚未被领取，不能提交结果",
            )
        if row["lease_owner"] != consumer:
            raise TaskRejected(
                RejectReason.LEASE_HELD_BY_OTHER,
                f"任务 {row['id']} 由 {row['lease_owner']!r} 持有，"
                f"消费者 {consumer!r} 无权操作",
            )
        if row["lease_expires_at"] is None or row["lease_expires_at"] <= now:
            raise TaskRejected(
                RejectReason.LEASE_EXPIRED,
                f"任务 {row['id']} 的租约已于 {row['lease_expires_at']} 过期"
                f"（当前 {now}），持过期租约不得提交结果",
            )
        if lease_token is not None:
            self._guard_token(row, lease_token, consumer)
        if expected_version is not None and expected_version != row["version"]:
            raise TaskRejected(
                RejectReason.VERSION_CONFLICT,
                f"版本冲突：客户端持有版本 {expected_version}，当前版本 {row['version']}",
            )
        return status

    @staticmethod
    def _guard_token(row: sqlite3.Row, lease_token: str, consumer: str) -> None:
        """校验执行凭证：空/格式不符 -> token_missing；属于旧租约世代 -> token_mismatch。"""
        if not lease_token:
            raise TaskRejected(
                RejectReason.TOKEN_MISSING,
                f"任务 {row['id']} 的操作缺少执行凭证；消费者 {consumer!r} "
                f"必须出示本次领取获得的 lease_token",
            )
        current = row["lease_token"]
        if current is None:
            raise TaskRejected(
                RejectReason.TOKEN_MISMATCH,
                f"任务 {row['id']} 当前租约没有有效凭证（租约信息异常），"
                f"消费者 {consumer!r} 出示的凭证不被接受",
            )
        if current != lease_token:
            raise TaskRejected(
                RejectReason.TOKEN_MISMATCH,
                f"任务 {row['id']} 的执行凭证已轮换：消费者 {consumer!r} 出示的凭证 "
                f"{_short_token(lease_token)} 不属于当前有效租约世代"
                f"（当前 {_short_token(current)}），该操作可能来自租约过期前的旧执行，"
                f"已拒绝以防覆盖后来者的执行",
            )

    def complete(
        self,
        task_id: str,
        consumer: str,
        result: dict[str, Any] | None = None,
        expected_version: int | None = None,
        lease_token: str | None = None,
    ) -> Task:
        now = self._now()
        with self._lock:
            row = self._fetch_by_id(task_id)
            if row is None:
                raise TaskRejected(
                    RejectReason.TASK_NOT_FOUND, f"task {task_id!r} 不存在"
                )
            self._guard_lease(row, consumer, now, expected_version, lease_token)
            body = json.dumps(result or {}, separators=(",", ":"), sort_keys=True)
            sql = (
                "UPDATE tasks SET status='succeeded', lease_owner=NULL, "
                "lease_expires_at=NULL, lease_token=NULL, result=?, last_error=NULL, "
                "version=version+1, updated_at=? "
                "WHERE id=? AND status='running' AND lease_owner=? AND version=? "
                "AND lease_expires_at>?"
            )
            params: list[Any] = [body, now, task_id, consumer, row["version"], now]
            if lease_token is not None:
                sql += " AND lease_token=?"
                params.append(lease_token)
            cur = self._conn.execute(sql, params)
            if cur.rowcount == 0:
                self._conn.rollback()
                raise TaskRejected(
                    RejectReason.VERSION_CONFLICT,
                    f"任务 {task_id} 在提交期间被并发修改，基于版本 {row['version']} 的写入被拒绝",
                )
            new_version = row["version"] + 1
            self._log_event(
                task_id, "complete", consumer, TaskStatus.RUNNING,
                TaskStatus.SUCCEEDED, new_version,
                f"状态流转 running->succeeded，消费者={consumer}，"
                f"凭证={_short_token(lease_token)}",
            )
            self._conn.commit()
            fresh = self._fetch_by_id(task_id, include_archive=False)
            assert fresh is not None
            return self._to_task(fresh)

    def fail(
        self,
        task_id: str,
        consumer: str,
        error: str = "",
        backoff_seconds: float | None = None,
        expected_version: int | None = None,
        lease_token: str | None = None,
    ) -> Task:
        now = self._now()
        with self._lock:
            row = self._fetch_by_id(task_id)
            if row is None:
                raise TaskRejected(
                    RejectReason.TASK_NOT_FOUND, f"task {task_id!r} 不存在"
                )
            self._guard_lease(row, consumer, now, expected_version, lease_token)

            token_cond = " AND lease_token=?" if lease_token is not None else ""
            token_arg: tuple[Any, ...] = (
                (lease_token,) if lease_token is not None else ()
            )

            # attempt 在领取时已 +1：首败 attempt=1。达到上限 -> 终态 FAILED。
            exhausted = row["attempt"] >= row["max_attempts"]
            if exhausted:
                cur = self._conn.execute(
                    "UPDATE tasks SET status='failed', lease_owner=NULL, "
                    "lease_expires_at=NULL, lease_token=NULL, last_error=?, "
                    "version=version+1, updated_at=? "
                    "WHERE id=? AND status='running' AND lease_owner=? "
                    "AND version=? AND lease_expires_at>?" + token_cond,
                    (error, now, task_id, consumer, row["version"], now, *token_arg),
                )
                to_status, reason_text = TaskStatus.FAILED, "重试次数耗尽，进入终态 failed"
            else:
                if backoff_seconds is None:
                    backoff_seconds = self.backoff_base_seconds * 2 ** (
                        row["attempt"] - 1
                    )
                available_at = now + backoff_seconds
                cur = self._conn.execute(
                    "UPDATE tasks SET status='pending', lease_owner=NULL, "
                    "lease_expires_at=NULL, lease_token=NULL, available_at=?, "
                    "last_error=?, version=version+1, updated_at=? "
                    "WHERE id=? AND status='running' AND lease_owner=? AND version=? "
                    "AND lease_expires_at>?" + token_cond,
                    (
                        available_at, error, now, task_id, consumer,
                        row["version"], now, *token_arg,
                    ),
                )
                to_status = TaskStatus.PENDING
                reason_text = (
                    f"可重试失败 running->pending，退避 {backoff_seconds}s 后可领取，"
                    f"attempt={row['attempt']}/{row['max_attempts']}"
                )
            if cur.rowcount == 0:
                self._conn.rollback()
                raise TaskRejected(
                    RejectReason.VERSION_CONFLICT,
                    f"任务 {task_id} 在失败回写期间被并发修改，基于版本 "
                    f"{row['version']} 的写入被拒绝",
                )
            new_version = row["version"] + 1
            self._log_event(
                task_id, "fail", consumer, TaskStatus.RUNNING, to_status,
                new_version, reason_text + (f"，error={error}" if error else "")
                + f"，凭证={_short_token(lease_token)}",
            )
            self._conn.commit()
            fresh = self._fetch_by_id(task_id, include_archive=False)
            assert fresh is not None
            return self._to_task(fresh)

    # ---------- 租约续期 / 释放 ----------

    def heartbeat(
        self,
        task_id: str,
        consumer: str,
        lease_seconds: float | None = None,
        lease_token: str | None = None,
    ) -> HeartbeatOutcome:
        ttl = lease_seconds if lease_seconds is not None else self.default_lease_seconds
        now = self._now()
        with self._lock:
            row = self._fetch_by_id(task_id)
            if row is None or TaskStatus(row["status"]) is not TaskStatus.RUNNING or row["lease_owner"] != consumer:
                outcome = HeartbeatOutcome.NOT_HELD
            elif row["lease_expires_at"] is None or row["lease_expires_at"] <= now:
                outcome = HeartbeatOutcome.EXPIRED
            elif lease_token is not None and (
                not lease_token or row["lease_token"] != lease_token
            ):
                # 凭证缺失或属于旧租约世代：不得续期，结论与提交侧一致。
                self._log_event(
                    task_id, "heartbeat.token_rejected", consumer,
                    TaskStatus.RUNNING, TaskStatus.RUNNING, row["version"],
                    f"续期被拒：凭证 {_short_token(lease_token)} 不属于当前租约世代 "
                    f"{_short_token(row['lease_token'])}",
                )
                outcome = HeartbeatOutcome.TOKEN_MISMATCH
            else:
                sql = (
                    "UPDATE tasks SET lease_expires_at=?, version=version+1, "
                    "updated_at=? WHERE id=? AND status='running' AND lease_owner=? "
                    "AND lease_expires_at>?"
                )
                params: list[Any] = [now + ttl, now, task_id, consumer, now]
                if lease_token is not None:
                    sql += " AND lease_token=?"
                    params.append(lease_token)
                cur = self._conn.execute(sql, params)
                if cur.rowcount == 0:
                    self._conn.rollback()
                    return HeartbeatOutcome.EXPIRED
                self._log_event(
                    task_id, "heartbeat", consumer, TaskStatus.RUNNING,
                    TaskStatus.RUNNING, row["version"] + 1,
                    f"租约续期 lease_expires_at={now + ttl} ttl={ttl}s "
                    f"凭证={_short_token(lease_token)}",
                )
                outcome = HeartbeatOutcome.RENEWED
            self._conn.commit()
            return outcome

    def release(
        self,
        task_id: str,
        consumer: str,
        lease_token: str | None = None,
    ) -> ReleaseOutcome:
        now = self._now()
        with self._lock:
            row = self._fetch_by_id(task_id)
            if (
                row is None
                or TaskStatus(row["status"]) is not TaskStatus.RUNNING
                or row["lease_owner"] != consumer
            ):
                self._conn.commit()
                return ReleaseOutcome.NOT_HELD
            if row["lease_expires_at"] is None or row["lease_expires_at"] <= now:
                # 租约已过期：不做任何状态改写，等待领取流程重新分配，归属判定明确。
                self._log_event(
                    task_id, "release.already_expired", consumer,
                    TaskStatus.RUNNING, TaskStatus.RUNNING, row["version"],
                    f"释放被拒：租约已于 {row['lease_expires_at']} 过期",
                )
                self._conn.commit()
                return ReleaseOutcome.ALREADY_EXPIRED
            if lease_token is not None and (
                not lease_token or row["lease_token"] != lease_token
            ):
                # 旧租约世代的凭证不得释放当前租约（即使同名消费者也不行）。
                self._log_event(
                    task_id, "release.token_rejected", consumer,
                    TaskStatus.RUNNING, TaskStatus.RUNNING, row["version"],
                    f"释放被拒：凭证 {_short_token(lease_token)} 不属于当前租约世代 "
                    f"{_short_token(row['lease_token'])}",
                )
                self._conn.commit()
                return ReleaseOutcome.TOKEN_MISMATCH

            sql = (
                "UPDATE tasks SET status='pending', lease_owner=NULL, "
                "lease_expires_at=NULL, lease_token=NULL, available_at=?, "
                "version=version+1, updated_at=? WHERE id=? AND status='running' "
                "AND lease_owner=? AND lease_expires_at>?"
            )
            params: list[Any] = [now, now, task_id, consumer, now]
            if lease_token is not None:
                sql += " AND lease_token=?"
                params.append(lease_token)
            cur = self._conn.execute(sql, params)
            if cur.rowcount == 0:
                self._conn.rollback()
                return ReleaseOutcome.ALREADY_EXPIRED
            self._log_event(
                task_id, "release", consumer, TaskStatus.RUNNING, TaskStatus.PENDING,
                row["version"] + 1,
                f"主动释放 running->pending，任务立即可重新领取，"
                f"凭证={_short_token(lease_token)}",
            )
            self._conn.commit()
            return ReleaseOutcome.RELEASED

    # ---------- 异常运行任务恢复原语 ----------

    def iter_running(self) -> list[sqlite3.Row]:
        """返回主表中全部 RUNNING 行（只读快照，含异常租约行）。"""
        with self._lock:
            cur = self._conn.execute(
                f"SELECT {', '.join(_PAYLOAD_COLUMNS)} FROM tasks "
                "WHERE status='running' ORDER BY updated_at, id"
            )
            return list(cur.fetchall())

    def requeue_expired_row(self, row: sqlite3.Row, now: float) -> bool:
        """把“租约确实过期”的 RUNNING 行条件性放回 PENDING。

        条件 UPDATE 保证：若扫描与恢复之间任务已被正常领取（仍有效），
        本次恢复 rowcount=0 不误伤。幂等：已是 PENDING 也返回 False。
        """
        with self._lock:
            cur = self._conn.execute(
                "UPDATE tasks SET status='pending', lease_owner=NULL, "
                "lease_expires_at=NULL, lease_token=NULL, available_at=?, "
                "version=version+1, updated_at=? "
                "WHERE id=? AND status='running' AND lease_owner IS NOT NULL "
                "AND lease_expires_at IS NOT NULL AND lease_token IS NOT NULL "
                "AND lease_expires_at<=? AND version=?",
                (now, now, row["id"], now, row["version"]),
            )
            changed = cur.rowcount > 0
            if changed:
                self._log_event(
                    row["id"], "recovery.requeue_expired", "recovery",
                    TaskStatus.RUNNING, TaskStatus.PENDING, row["version"] + 1,
                    f"运维恢复：过期租约（到期={row['lease_expires_at']}，"
                    f"持有者={row['lease_owner']!r}）放回 PENDING",
                )
            self._conn.commit()
            return changed

    def quarantine_corrupt_row(self, row: sqlite3.Row, detail: str) -> bool:
        """把租约信息异常的 RUNNING 行条件性隔离为 QUARANTINED。"""
        with self._lock:
            cur = self._conn.execute(
                "UPDATE tasks SET status='quarantined', lease_owner=NULL, "
                "lease_expires_at=NULL, lease_token=NULL, version=version+1, "
                "updated_at=? "
                "WHERE id=? AND status='running' AND version=? "
                "AND (lease_owner IS NULL OR lease_expires_at IS NULL "
                "OR lease_token IS NULL)",
                (self._now(), row["id"], row["version"]),
            )
            changed = cur.rowcount > 0
            if changed:
                self._log_event(
                    row["id"], "recovery.quarantine", "recovery",
                    TaskStatus.RUNNING, TaskStatus.QUARANTINED,
                    row["version"] + 1, f"运维隔离：租约信息异常（{detail}）",
                )
            self._conn.commit()
            return changed

    def requeue_corrupt_row(self, row: sqlite3.Row, detail: str, now: float) -> bool:
        """把租约信息异常的 RUNNING 行直接条件性放回 PENDING（按策略）。"""
        with self._lock:
            cur = self._conn.execute(
                "UPDATE tasks SET status='pending', lease_owner=NULL, "
                "lease_expires_at=NULL, lease_token=NULL, available_at=?, "
                "version=version+1, updated_at=? "
                "WHERE id=? AND status='running' AND version=? "
                "AND (lease_owner IS NULL OR lease_expires_at IS NULL "
                "OR lease_token IS NULL)",
                (now, now, row["id"], row["version"]),
            )
            changed = cur.rowcount > 0
            if changed:
                self._log_event(
                    row["id"], "recovery.requeue_corrupt", "recovery",
                    TaskStatus.RUNNING, TaskStatus.PENDING, row["version"] + 1,
                    f"运维恢复：租约信息异常（{detail}）直接放回 PENDING",
                )
            self._conn.commit()
            return changed

    def requeue_quarantined(
        self, task_id: str, *, expected_version: int | None = None
    ) -> Task:
        """人工把 QUARANTINED 任务放回 PENDING，重新进入可领取队列。"""
        now = self._now()
        with self._lock:
            row = self._fetch_by_id(task_id, include_archive=False)
            if row is None:
                raise TaskRejected(
                    RejectReason.TASK_NOT_FOUND, f"task {task_id!r} 不存在"
                )
            status = TaskStatus(row["status"])
            if status is not TaskStatus.QUARANTINED:
                raise TaskRejected(
                    RejectReason.NOT_PENDING_OR_UNCLAIMED,
                    f"任务 {task_id} 当前为 {status.value}，"
                    f"只有 quarantined 状态可人工放回",
                )
            if expected_version is not None and expected_version != row["version"]:
                raise TaskRejected(
                    RejectReason.VERSION_CONFLICT,
                    f"版本冲突：客户端持有版本 {expected_version}，"
                    f"当前版本 {row['version']}",
                )
            cur = self._conn.execute(
                "UPDATE tasks SET status='pending', lease_owner=NULL, "
                "lease_expires_at=NULL, lease_token=NULL, available_at=?, "
                "version=version+1, updated_at=? "
                "WHERE id=? AND status='quarantined' AND version=?",
                (now, now, task_id, row["version"]),
            )
            if cur.rowcount == 0:
                self._conn.rollback()
                raise TaskRejected(
                    RejectReason.VERSION_CONFLICT,
                    f"任务 {task_id} 在人工放回期间被并发修改",
                )
            self._log_event(
                task_id, "recovery.manual_requeue", "recovery",
                TaskStatus.QUARANTINED, TaskStatus.PENDING, row["version"] + 1,
                "人工确认后将隔离任务放回 PENDING",
            )
            self._conn.commit()
            fresh = self._fetch_by_id(task_id, include_archive=False)
            assert fresh is not None
            return self._to_task(fresh)

    # ---------- 终态任务归档原语 ----------

    def archive_terminal_candidates(
        self, *, older_than_seconds: float | None, limit: int, now: float
    ) -> list[sqlite3.Row]:
        """选取主表中可归档的终态行（可要求终结足够久），只读。"""
        sql = (
            f"SELECT {', '.join(_PAYLOAD_COLUMNS)} FROM tasks "
            "WHERE status IN ('succeeded','failed')"
        )
        params: list[Any] = []
        if older_than_seconds is not None:
            sql += " AND updated_at<=?"
            params.append(now - older_than_seconds)
        sql += " ORDER BY updated_at, id LIMIT ?"
        params.append(limit)
        with self._lock:
            return list(self._conn.execute(sql, params).fetchall())

    def archive_row(self, row: sqlite3.Row, now: float) -> bool:
        """把单条终态行原子迁入 tasks_archive（先插后删，单条小事务）。

        重复执行：归档表已存在同 id 时按幂等处理删除主表残留；
        非终态行拒绝迁移。返回是否实际发生迁移。
        """
        with self._lock:
            status = TaskStatus(row["status"])
            if status not in (TaskStatus.SUCCEEDED, TaskStatus.FAILED):
                return False
            cols = ", ".join(_PAYLOAD_COLUMNS)
            placeholders = ", ".join("?" for _ in _PAYLOAD_COLUMNS)
            values = [row[c] for c in _PAYLOAD_COLUMNS]
            try:
                self._conn.execute(
                    f"INSERT INTO tasks_archive ({cols}, archived_at) "
                    f"VALUES ({placeholders}, ?)",
                    (*values, now),
                )
            except sqlite3.IntegrityError:
                # 已归档过（重复执行）：不重复插入，继续清理主表可能的残留。
                self._conn.rollback()
            self._conn.execute("DELETE FROM tasks WHERE id=? AND status IN "
                               "('succeeded','failed')", (row["id"],))
            self._log_event(
                row["id"], "archive", "archive", status, status,
                row["version"],
                f"终态任务归档到 tasks_archive，归档时刻={now}",
            )
            self._conn.commit()
            return True

    def result_row(self, task_id: str) -> sqlite3.Row | None:
        """读取任务的完整行（含 result 列），主表优先、归档表兜底。"""
        with self._lock:
            cur = self._conn.execute(
                f"SELECT {', '.join(_PAYLOAD_COLUMNS)} FROM tasks WHERE id=?",
                (task_id,),
            )
            row = cur.fetchone()
            if row is not None:
                return row
            cur = self._conn.execute(
                f"SELECT {', '.join(_PAYLOAD_COLUMNS)}, archived_at "
                "FROM tasks_archive WHERE id=?",
                (task_id,),
            )
            return cur.fetchone()
