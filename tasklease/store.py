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
    ReleaseOutcome,
    RecoverOutcome,
    RejectReason,
    RunningLeaseState,
    TaskRejected,
)
from .models import EventRecord, Page, Task, TaskStatus

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
    "archived_at",
    "created_at",
    "updated_at",
)


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

    def _to_task(self, row: sqlite3.Row) -> Task:
        data = dict(row)
        data["status"] = TaskStatus(data["status"])
        data["payload"] = json.loads(data.get("payload") or "{}")
        result = data.get("result")
        data["result"] = json.loads(result) if result else None
        data.setdefault("lease_token", None)
        data.setdefault("archived_at", None)
        data.pop("_rid", None)
        return Task(**data)

    def _to_event(self, row: sqlite3.Row, archived: bool = False) -> EventRecord:
        data = dict(row)
        data.setdefault("lease_token", None)
        data["archived"] = archived
        return EventRecord(**data)

    def _fetch_by_id(self, task_id: str) -> sqlite3.Row | None:
        cur = self._conn.execute(
            f"SELECT {', '.join(_PAYLOAD_COLUMNS)} FROM tasks WHERE id=?",
            (task_id,),
        )
        return cur.fetchone()

    def _fetch_archived_by_id(self, task_id: str) -> sqlite3.Row | None:
        cur = self._conn.execute(
            f"SELECT {', '.join(_PAYLOAD_COLUMNS)} FROM tasks_archive WHERE id=?",
            (task_id,),
        )
        return cur.fetchone()

    def _fetch_by_key(self, key: str) -> sqlite3.Row | None:
        cur = self._conn.execute(
            f"SELECT {', '.join(_PAYLOAD_COLUMNS)} FROM tasks "
            "WHERE idempotency_key=?",
            (key,),
        )
        return cur.fetchone()

    def _fetch_archived_by_key(self, key: str) -> sqlite3.Row | None:
        cur = self._conn.execute(
            f"SELECT {', '.join(_PAYLOAD_COLUMNS)} FROM tasks_archive "
            "WHERE idempotency_key=?",
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
        lease_token: str | None = None,
    ) -> None:
        self._conn.execute(
            "INSERT INTO task_events (task_id, event, actor, lease_token, "
            "from_status, to_status, version, reason, created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (
                task_id,
                event,
                actor,
                lease_token,
                from_status.value if from_status else None,
                to_status.value if to_status else None,
                version,
                reason,
                self._now(),
            ),
        )

    # ---------- 查询 ----------

    def get(self, task_id: str, include_archive: bool = True) -> Task:
        with self._lock:
            row = self._fetch_by_id(task_id)
            if row is None and include_archive:
                row = self._fetch_archived_by_id(task_id)
            if row is None:
                raise TaskRejected(
                    RejectReason.TASK_NOT_FOUND, f"task {task_id!r} 不存在"
                )
            return self._to_task(row)

    def get_by_idempotency_key(
        self, key: str, include_archive: bool = True
    ) -> Task | None:
        with self._lock:
            row = self._fetch_by_key(key)
            if row is None and include_archive:
                row = self._fetch_archived_by_key(key)
            return self._to_task(row) if row is not None else None

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
            return self._to_task(existing), EnqueueOutcome.EXISTING
        # 已归档的终态任务：重复入队的既有结论不受归档影响。
        archived = self._fetch_archived_by_key(key)
        if archived is not None:
            self._reject_if_enqueue_blocked(archived, now, key)

        task_id = uuid.uuid4().hex
        body = json.dumps(payload, separators=(",", ":"), sort_keys=True)
        try:
            self._conn.execute(
                "INSERT INTO tasks (id, idempotency_key, payload, status, attempt, "
                "max_attempts, version, lease_owner, lease_expires_at, lease_token, "
                "available_at, last_error, result, archived_at, created_at, updated_at) "
                "VALUES (?,?,?,'pending',0,?,0,NULL,NULL,NULL,?,NULL,NULL,NULL,?,?)",
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
            return self._to_task(existing), EnqueueOutcome.EXISTING

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
        if status is TaskStatus.QUARANTINED:
            raise TaskRejected(
                RejectReason.TASK_QUARANTINED,
                f"key={key} 的任务处于隔离状态（租约信息异常，等待人工处理），"
                "不得重复入队",
            )
        if status is TaskStatus.RUNNING:
            expires = row["lease_expires_at"]
            owner = row["lease_owner"]
            token = row["lease_token"]
            if expires is None or not owner or not token:
                # 旧版本升级遗留的异常 RUNNING：需先走恢复入口，不允许重复入队。
                raise TaskRejected(
                    RejectReason.LEASE_CORRUPT,
                    f"key={key} 的任务处于 running 但租约信息缺失"
                    f"（owner={owner!r}, expires={expires}, token 缺失），"
                    "请先通过恢复入口处理",
                )
            if expires > now:
                raise TaskRejected(
                    RejectReason.TASK_EXECUTING,
                    f"key={key} 的任务正在被 {owner} 执行，"
                    f"租约到期时间={expires}（当前 {now}），不得重复入队",
                )
            # RUNNING 但租约已过期：任务将被重新领取，重复提交直接收敛到同一任务。

    # ---------- 并发领取 ----------

    def acquire(
        self, consumer: str, lease_seconds: float | None = None
    ) -> tuple[Task, str] | None:
        """原子领取一个任务，返回 ``(任务, 本次执行凭证)``；无任务返回 None。

        凭证只属于本次领取：一旦租约因过期/释放/完成而轮换，旧凭证立刻失效。
        """
        if not consumer:
            raise ValueError("consumer 不能为空")
        ttl = lease_seconds if lease_seconds is not None else self.default_lease_seconds
        if ttl <= 0:
            raise ValueError("lease_seconds 必须 > 0")
        now = self._now()
        expires = now + ttl
        with self._lock:
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
                           AND lease_expires_at IS NOT NULL
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
                f"ttl={ttl}s lease_token={token}",
                lease_token=token,
            )
            self._conn.commit()
            row = self._fetch_by_id(task_id)
            assert row is not None
            return self._to_task(row), token

    # ---------- 结果提交 ----------

    def _guard_lease(
        self,
        row: sqlite3.Row,
        consumer: str,
        token: str | None,
        now: float,
        expected_version: int | None,
    ) -> TaskStatus:
        """校验状态机位置、租约归属、执行凭证、有效期、客户端版本。

        判定顺序刻意可区分：
        1. 终态 / 未领取；
        2. 租约信息缺失（旧版本升级遗留，lease_corrupt）；
        3. 持有者不是本人（lease_held_by_other，已被别人接管）；
        4. 同消费者名但凭证对不上（invalid_token，旧一轮执行）；
        5. 租约已过期（lease_expired）；
        6. 版本冲突（version_conflict）。
        """
        task_id = row["id"]
        status = TaskStatus(row["status"])
        if status in (TaskStatus.SUCCEEDED, TaskStatus.FAILED):
            raise TaskRejected(
                RejectReason.TASK_TERMINAL,
                f"任务 {task_id} 已处于终态 {status.value}，状态流转被拒绝",
            )
        if status is TaskStatus.QUARANTINED:
            raise TaskRejected(
                RejectReason.TASK_QUARANTINED,
                f"任务 {task_id} 已被隔离（租约信息异常，等待人工处理），不能提交结果",
            )
        if status is not TaskStatus.RUNNING:
            raise TaskRejected(
                RejectReason.NOT_PENDING_OR_UNCLAIMED,
                f"任务 {task_id} 当前为 {status.value}，尚未被领取，不能提交结果",
            )
        owner = row["lease_owner"]
        expires = row["lease_expires_at"]
        current_token = row["lease_token"]
        if not owner or expires is None or not current_token:
            raise TaskRejected(
                RejectReason.LEASE_CORRUPT,
                f"任务 {task_id} 处于 running 但租约信息缺失"
                f"（owner={owner!r}, expires={expires}, token 缺失），"
                "本次执行不被承认，请走恢复入口",
            )
        if owner != consumer:
            raise TaskRejected(
                RejectReason.LEASE_HELD_BY_OTHER,
                f"任务 {task_id} 在 {expires} 前由 {owner!r} 持有"
                f"（当前 {now}），消费者 {consumer!r} 已丧失执行权",
            )
        if token != current_token:
            raise TaskRejected(
                RejectReason.INVALID_TOKEN,
                f"消费者 {consumer!r} 出示的执行凭证已失效：任务 {task_id} "
                "的租约已轮换给同消费者名的新一轮执行，旧一轮的提交不得覆盖新产出",
            )
        if expires <= now:
            raise TaskRejected(
                RejectReason.LEASE_EXPIRED,
                f"任务 {task_id} 的租约已于 {expires} 过期（当前 {now}），"
                "持过期租约不得提交结果",
            )
        if expected_version is not None and expected_version != row["version"]:
            raise TaskRejected(
                RejectReason.VERSION_CONFLICT,
                f"版本冲突：客户端持有版本 {expected_version}，当前版本 {row['version']}",
            )
        return status

    def complete(
        self,
        task_id: str,
        consumer: str,
        token: str,
        result: dict[str, Any] | None = None,
        expected_version: int | None = None,
    ) -> Task:
        now = self._now()
        with self._lock:
            row = self._fetch_by_id(task_id)
            if row is None:
                if self._fetch_archived_by_id(task_id) is not None:
                    raise TaskRejected(
                        RejectReason.TASK_TERMINAL,
                        f"任务 {task_id} 已终结并归档，终态不可再提交",
                    )
                raise TaskRejected(
                    RejectReason.TASK_NOT_FOUND, f"task {task_id!r} 不存在"
                )
            self._guard_lease(row, consumer, token, now, expected_version)
            body = json.dumps(result or {}, separators=(",", ":"), sort_keys=True)
            cur = self._conn.execute(
                "UPDATE tasks SET status='succeeded', lease_owner=NULL, "
                "lease_expires_at=NULL, lease_token=NULL, result=?, last_error=NULL, "
                "version=version+1, updated_at=? "
                "WHERE id=? AND status='running' AND lease_owner=? AND lease_token=? "
                "AND version=? AND lease_expires_at>?",
                (body, now, task_id, consumer, token, row["version"], now),
            )
            if cur.rowcount == 0:
                self._conn.rollback()
                raise TaskRejected(
                    RejectReason.INVALID_TOKEN,
                    f"任务 {task_id} 在提交期间发生租约轮换，凭证 {token} 已失效，"
                    "提交被拒绝以避免覆盖新执行",
                )
            new_version = row["version"] + 1
            self._log_event(
                task_id, "complete", consumer, TaskStatus.RUNNING,
                TaskStatus.SUCCEEDED, new_version,
                f"状态流转 running->succeeded，消费者={consumer}",
                lease_token=token,
            )
            self._conn.commit()
            fresh = self._fetch_by_id(task_id)
            assert fresh is not None
            return self._to_task(fresh)

    def fail(
        self,
        task_id: str,
        consumer: str,
        token: str,
        error: str = "",
        backoff_seconds: float | None = None,
        expected_version: int | None = None,
    ) -> Task:
        now = self._now()
        with self._lock:
            row = self._fetch_by_id(task_id)
            if row is None:
                raise TaskRejected(
                    RejectReason.TASK_NOT_FOUND, f"task {task_id!r} 不存在"
                )
            self._guard_lease(row, consumer, token, now, expected_version)

            # attempt 在领取时已 +1：首败 attempt=1。达到上限 -> 终态 FAILED。
            exhausted = row["attempt"] >= row["max_attempts"]
            guard_sql = (
                "AND status='running' AND lease_owner=? AND lease_token=? "
                "AND version=? AND lease_expires_at>?"
            )
            guard_args = (consumer, token, row["version"], now)
            if exhausted:
                cur = self._conn.execute(
                    "UPDATE tasks SET status='failed', lease_owner=NULL, "
                    "lease_expires_at=NULL, lease_token=NULL, last_error=?, "
                    "version=version+1, updated_at=? WHERE id=? " + guard_sql,
                    (error, now, task_id, *guard_args),
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
                    "WHERE id=? " + guard_sql,
                    (available_at, error, now, task_id, *guard_args),
                )
                to_status = TaskStatus.PENDING
                reason_text = (
                    f"可重试失败 running->pending，退避 {backoff_seconds}s 后可领取，"
                    f"attempt={row['attempt']}/{row['max_attempts']}"
                )
            if cur.rowcount == 0:
                self._conn.rollback()
                raise TaskRejected(
                    RejectReason.INVALID_TOKEN,
                    f"任务 {task_id} 在失败回写期间发生租约轮换，凭证 {token} 已失效",
                )
            new_version = row["version"] + 1
            self._log_event(
                task_id, "fail", consumer, TaskStatus.RUNNING, to_status,
                new_version, reason_text + (f"，error={error}" if error else ""),
                lease_token=token,
            )
            self._conn.commit()
            fresh = self._fetch_by_id(task_id)
            assert fresh is not None
            return self._to_task(fresh)

    # ---------- 租约续期 / 释放 ----------

    def heartbeat(
        self,
        task_id: str,
        consumer: str,
        token: str,
        lease_seconds: float | None = None,
    ) -> HeartbeatOutcome:
        ttl = lease_seconds if lease_seconds is not None else self.default_lease_seconds
        now = self._now()
        with self._lock:
            row = self._fetch_by_id(task_id)
            if row is None or TaskStatus(row["status"]) is not TaskStatus.RUNNING:
                outcome = HeartbeatOutcome.NOT_HELD
            elif not row["lease_owner"] or not row["lease_token"] or \
                    row["lease_expires_at"] is None:
                outcome = HeartbeatOutcome.CORRUPT
            elif row["lease_owner"] != consumer:
                outcome = HeartbeatOutcome.NOT_HELD
            elif token != row["lease_token"]:
                # 同消费者名的旧一轮执行：凭证已随租约轮换失效。
                outcome = HeartbeatOutcome.INVALID_TOKEN
            elif row["lease_expires_at"] <= now:
                outcome = HeartbeatOutcome.EXPIRED
            else:
                cur = self._conn.execute(
                    "UPDATE tasks SET lease_expires_at=?, version=version+1, "
                    "updated_at=? WHERE id=? AND status='running' AND lease_owner=? "
                    "AND lease_token=? AND lease_expires_at>?",
                    (now + ttl, now, task_id, consumer, token, now),
                )
                if cur.rowcount == 0:
                    self._conn.rollback()
                    return HeartbeatOutcome.EXPIRED
                self._log_event(
                    task_id, "heartbeat", consumer, TaskStatus.RUNNING,
                    TaskStatus.RUNNING, row["version"] + 1,
                    f"租约续期 lease_expires_at={now + ttl} ttl={ttl}s",
                    lease_token=token,
                )
                outcome = HeartbeatOutcome.RENEWED
            self._conn.commit()
            return outcome

    def release(self, task_id: str, consumer: str, token: str) -> ReleaseOutcome:
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
            if not row["lease_owner"] or not row["lease_token"] or \
                    row["lease_expires_at"] is None:
                self._conn.commit()
                return ReleaseOutcome.CORRUPT
            if token != row["lease_token"]:
                self._conn.commit()
                return ReleaseOutcome.INVALID_TOKEN
            if row["lease_expires_at"] <= now:
                # 租约已过期：不做任何状态改写，等待领取流程重新分配，归属判定明确。
                self._log_event(
                    task_id, "release.already_expired", consumer,
                    TaskStatus.RUNNING, TaskStatus.RUNNING, row["version"],
                    f"释放被拒：租约已于 {row['lease_expires_at']} 过期",
                    lease_token=token,
                )
                self._conn.commit()
                return ReleaseOutcome.ALREADY_EXPIRED

            cur = self._conn.execute(
                "UPDATE tasks SET status='pending', lease_owner=NULL, "
                "lease_expires_at=NULL, lease_token=NULL, available_at=?, "
                "version=version+1, updated_at=? WHERE id=? AND status='running' "
                "AND lease_owner=? AND lease_token=? AND lease_expires_at>?",
                (now, now, task_id, consumer, token, now),
            )
            if cur.rowcount == 0:
                self._conn.rollback()
                return ReleaseOutcome.ALREADY_EXPIRED
            self._log_event(
                task_id, "release", consumer, TaskStatus.RUNNING, TaskStatus.PENDING,
                row["version"] + 1, "主动释放 running->pending，任务立即可重新领取",
                lease_token=token,
            )
            self._conn.commit()
            return ReleaseOutcome.RELEASED

    # ---------- 异常 RUNNING 任务恢复 ----------

    def classify_running(self, task_id: str) -> RunningLeaseState:
        """对单条 RUNNING 任务做三分类：active / expired / corrupt。

        - active：租约字段完整且未过期，仍被有效持有，恢复动作不得碰它；
        - expired：字段完整但已过期，可安全地重新置回可领取；
        - corrupt：租约字段缺失（旧版本升级遗留），需隔离人工处理。
        """
        with self._lock:
            row = self._fetch_by_id(task_id)
            if row is None:
                raise TaskRejected(
                    RejectReason.TASK_NOT_FOUND, f"task {task_id!r} 不存在"
                )
            if TaskStatus(row["status"]) is not TaskStatus.RUNNING:
                raise TaskRejected(
                    RejectReason.NOT_PENDING_OR_UNCLAIMED,
                    f"任务 {task_id} 当前状态 {row['status']}，不是 running",
                )
            return self._classify_row(row)

    def _classify_row(self, row: sqlite3.Row) -> RunningLeaseState:
        if not row["lease_owner"] or not row["lease_token"] or \
                row["lease_expires_at"] is None:
            return RunningLeaseState.CORRUPT
        if row["lease_expires_at"] <= self._now():
            return RunningLeaseState.EXPIRED
        return RunningLeaseState.ACTIVE

    def list_stuck_running(self, limit: int = 500) -> list[Task]:
        """列出需要运维关注的 RUNNING：租约已过期，或租约信息异常。

        字段完整且仍被有效持有的任务不会出现（不会误伤正常执行）。
        """
        now = self._now()
        with self._lock:
            cols = ", ".join(_PAYLOAD_COLUMNS)
            cur = self._conn.execute(
                f"SELECT {cols} FROM tasks WHERE status='running' AND ("
                "lease_expires_at IS NULL OR lease_owner IS NULL OR lease_owner='' "
                "OR lease_token IS NULL OR lease_token='' OR lease_expires_at<=?) "
                "ORDER BY updated_at, id LIMIT ?",
                (now, limit),
            )
            return [self._to_task(r) for r in cur.fetchall()]

    def recover_stuck(
        self,
        task_id: str | None = None,
        *,
        batch_size: int = 100,
        dry_run: bool = False,
        quarantine_corrupt: bool = True,
    ) -> list[tuple[Task, RecoverOutcome]]:
        """幂等恢复异常 RUNNING 任务，可反复执行而无额外副作用。

        - 租约已过期（字段完整）：置回 PENDING、available_at=now，立即可重新领取；
        - 租约信息缺失：``quarantine_corrupt=True`` 时标记为 QUARANTINED（人工处理），
          否则也置回 PENDING 重新领取（自动兜底）；
        - 仍被有效持有的任务：跳过（SKIPPED_ACTIVE），绝不改写。

        每批独立短事务提交，恢复过程不持有长事务，正常的领取和提交不会被卡住。
        ``dry_run=True`` 只返回将执行的判定，不落任何写入。
        """
        results: list[tuple[Task, RecoverOutcome]] = []
        now = self._now()
        with self._lock:
            rows = self._select_stuck_rows(task_id, batch_size)
            for row in rows:
                state = self._classify_row(row)
                task = self._to_task(row)
                if state is RunningLeaseState.ACTIVE:
                    results.append((task, RecoverOutcome.SKIPPED_ACTIVE))
                    continue
                if dry_run:
                    outcome = (
                        RecoverOutcome.QUARANTINED
                        if state is RunningLeaseState.CORRUPT and quarantine_corrupt
                        else RecoverOutcome.REQUEUED
                    )
                    results.append((task, outcome))
                    continue
                if state is RunningLeaseState.CORRUPT and quarantine_corrupt:
                    new_version = self._quarantine_one(row, now)
                    results.append((task, RecoverOutcome.QUARANTINED))
                else:
                    new_version = self._requeue_one(row, now)
                    results.append((task, RecoverOutcome.REQUEUED))
            self._conn.commit()
        return results

    def _select_stuck_rows(
        self, task_id: str | None, batch_size: int
    ) -> list[sqlite3.Row]:
        cols = ", ".join(_PAYLOAD_COLUMNS)
        params: dict[str, Any] = {"now": self._now(), "lim": batch_size}
        if task_id is not None:
            # 显式指定单任务时：即便它仍被有效持有也要选出来，
            # 交由 classify 判定为 active 并跳过（SKIPPED_ACTIVE），结论可解释。
            cur = self._conn.execute(
                f"SELECT {cols} FROM tasks WHERE status='running' AND id=:tid",
                {**params, "tid": task_id},
            )
            return list(cur.fetchall())
        cur = self._conn.execute(
            f"SELECT {cols} FROM tasks WHERE status='running' AND ("
            "lease_expires_at IS NULL OR lease_owner IS NULL OR lease_owner='' "
            "OR lease_token IS NULL OR lease_token='' OR lease_expires_at<=:now) "
            "ORDER BY updated_at, id LIMIT :lim",
            params,
        )
        return list(cur.fetchall())

    def _requeue_one(self, row: sqlite3.Row, now: float) -> int:
        """把一条过期/异常 RUNNING 置回 PENDING。

        条件 UPDATE 只匹配“非有效持有”的行：若恰好在判定后被别人续期/接管，
        rowcount=0，本次恢复跳过，绝不误伤正在执行的任务。
        """
        cur = self._conn.execute(
            "UPDATE tasks SET status='pending', lease_owner=NULL, "
            "lease_expires_at=NULL, lease_token=NULL, available_at=?, "
            "last_error=COALESCE(last_error,?), version=version+1, updated_at=? "
            "WHERE id=? AND status='running' AND ("
            "lease_expires_at IS NULL OR lease_owner IS NULL OR lease_owner='' "
            "OR lease_token IS NULL OR lease_token='' OR lease_expires_at<=?)",
            (
                now,
                "租约异常后由恢复入口重置",
                now,
                row["id"],
                now,
            ),
        )
        new_version = row["version"] + 1
        if cur.rowcount:
            self._log_event(
                row["id"], "recover.requeue", "ops",
                TaskStatus.RUNNING, TaskStatus.PENDING, new_version,
                f"恢复入口将异常/过期 RUNNING 置回 pending（expires={row['lease_expires_at']}）",
            )
        return new_version

    def _quarantine_one(self, row: sqlite3.Row, now: float) -> int:
        cur = self._conn.execute(
            "UPDATE tasks SET status='quarantined', lease_owner=NULL, "
            "lease_expires_at=NULL, lease_token=NULL, "
            "last_error=COALESCE(last_error,?), version=version+1, updated_at=? "
            "WHERE id=? AND status='running' AND ("
            "lease_expires_at IS NULL OR lease_owner IS NULL OR lease_owner='' "
            "OR lease_token IS NULL OR lease_token='')",
            ("租约信息异常，由恢复入口隔离，等待人工处理", now, row["id"]),
        )
        new_version = row["version"] + 1
        if cur.rowcount:
            self._log_event(
                row["id"], "recover.quarantine", "ops",
                TaskStatus.RUNNING, TaskStatus.QUARANTINED, new_version,
                f"租约字段缺失已隔离（owner={row['lease_owner']!r}, "
                f"expires={row['lease_expires_at']}, token 缺失）",
            )
        return new_version

    def requeue_quarantined(self, task_id: str) -> Task:
        """人工确认后把隔离任务重新放回可领取队列（幂等）。"""
        now = self._now()
        with self._lock:
            row = self._fetch_by_id(task_id)
            if row is None:
                raise TaskRejected(
                    RejectReason.TASK_NOT_FOUND, f"task {task_id!r} 不存在"
                )
            if TaskStatus(row["status"]) is not TaskStatus.QUARANTINED:
                raise TaskRejected(
                    RejectReason.NOT_PENDING_OR_UNCLAIMED,
                    f"任务 {task_id} 不是 quarantined（当前 {row['status']}）",
                )
            self._conn.execute(
                "UPDATE tasks SET status='pending', available_at=?, "
                "version=version+1, updated_at=? WHERE id=? AND status='quarantined'",
                (now, now, task_id),
            )
            self._log_event(
                task_id, "recover.quarantine_requeue", "ops",
                TaskStatus.QUARANTINED, TaskStatus.PENDING, row["version"] + 1,
                "人工确认后隔离任务重新放回可领取队列",
            )
            self._conn.commit()
            fresh = self._fetch_by_id(task_id)
            assert fresh is not None
            return self._to_task(fresh)

    # ---------- 结果与执行历史读回 ----------

    def get_result(self, task_id: str, include_archive: bool = True) -> dict[str, Any]:
        """读回任务最终结论：状态、产出（成功）或失败原因（失败）。"""
        with self._lock:
            row = self._fetch_by_id(task_id)
            archived = False
            if row is None and include_archive:
                row = self._fetch_archived_by_id(task_id)
                archived = row is not None
            if row is None:
                raise TaskRejected(
                    RejectReason.TASK_NOT_FOUND, f"task {task_id!r} 不存在"
                )
            result_raw = row["result"]
            return {
                "task_id": task_id,
                "idempotency_key": row["idempotency_key"],
                "status": TaskStatus(row["status"]).value,
                "result": json.loads(result_raw) if result_raw else None,
                "last_error": row["last_error"],
                "attempt": row["attempt"],
                "max_attempts": row["max_attempts"],
                "updated_at": row["updated_at"],
                "archived": archived,
            }

    def list_events(
        self,
        task_id: str,
        *,
        limit: int = 100,
        include_archive: bool = True,
    ) -> list[EventRecord]:
        """读回单任务完整执行历史（领取/续期/释放/失败/完成/恢复/归档）。

        归档后的事件同样可读：对主表与归档表做 UNION ALL，按原始事件序号排序。
        """
        if limit <= 0:
            raise ValueError("limit 必须 > 0")
        cols = (
            "id, task_id, event, actor, lease_token, from_status, to_status, "
            "version, reason, created_at"
        )
        with self._lock:
            live = list(
                self._conn.execute(
                    f"SELECT {cols} FROM task_events WHERE task_id=? "
                    "ORDER BY id LIMIT ?",
                    (task_id, limit),
                )
            )
            archived_rows: list[sqlite3.Row] = []
            if include_archive:
                archived_rows = list(
                    self._conn.execute(
                        f"SELECT {cols} FROM task_events_archive "
                        "WHERE task_id=? ORDER BY id LIMIT ?",
                        (task_id, limit),
                    )
                )
            events = [self._to_event(r, archived=False) for r in live]
            events += [self._to_event(r, archived=True) for r in archived_rows]
            events.sort(key=lambda e: (e.id, e.archived))
            return events[:limit]

    def list_tasks(
        self,
        *,
        statuses: list[TaskStatus] | None = None,
        updated_from: float | None = None,
        updated_to: float | None = None,
        created_from: float | None = None,
        created_to: float | None = None,
        limit: int = 50,
        cursor: int | None = None,
    ) -> Page:
        """分页筛选主表任务。游标为底层 rowid（单调递增），``next_cursor`` 为 None 即到尾。

        已归档任务请用 :meth:`list_archived_tasks`。
        """
        if limit <= 0:
            raise ValueError("limit 必须 > 0")
        where: list[str] = []
        params: dict[str, Any] = {"lim": limit + 1, "cur": cursor or 0}
        if statuses:
            where.append("status IN (%s)" % ",".join(
                f":s{i}" for i in range(len(statuses))
            ))
            for i, st in enumerate(statuses):
                params[f"s{i}"] = st.value
        for col, lo, hi in (
            ("updated_at", updated_from, updated_to),
            ("created_at", created_from, created_to),
        ):
            if lo is not None:
                where.append(f"{col}>=:{col}_lo")
                params[f"{col}_lo"] = lo
            if hi is not None:
                where.append(f"{col}<=:{col}_hi")
                params[f"{col}_hi"] = hi
        where_sql = (" WHERE " + " AND ".join(where)) if where else ""
        cols = ", ".join(_PAYLOAD_COLUMNS)
        with self._lock:
            # rowid 游标与过滤条件统一在 where 列表处理。
            clauses = list(where) + ["rowid>:cur"]
            where_sql = " WHERE " + " AND ".join(clauses)
            sql_live = (
                f"SELECT {cols}, rowid AS _rid FROM tasks{where_sql} "
                "ORDER BY rowid LIMIT :lim"
            )
            rows = list(self._conn.execute(sql_live, params))
            items: list[tuple[Task, int]] = [
                (self._to_task(r), r["_rid"]) for r in rows
            ]
            has_more = len(items) > limit
            items = items[:limit]
            next_cursor = items[-1][1] if has_more else None
            return Page(
                items=[t for t, _ in items], limit=limit, next_cursor=next_cursor
            )

    def list_archived_tasks(
        self,
        *,
        statuses: list[TaskStatus] | None = None,
        updated_from: float | None = None,
        updated_to: float | None = None,
        created_from: float | None = None,
        created_to: float | None = None,
        limit: int = 50,
        cursor: int | None = None,
    ) -> Page:
        """对归档表分页筛选（参数语义同 :meth:`list_tasks`，游标为归档表 rowid）。"""
        if limit <= 0:
            raise ValueError("limit 必须 > 0")
        where: list[str] = ["rowid>:cur"]
        params: dict[str, Any] = {"lim": limit + 1, "cur": cursor or 0}
        if statuses:
            where.append("status IN (%s)" % ",".join(
                f":s{i}" for i in range(len(statuses))
            ))
            for i, st in enumerate(statuses):
                params[f"s{i}"] = st.value
        for col, lo, hi in (
            ("updated_at", updated_from, updated_to),
            ("created_at", created_from, created_to),
        ):
            if lo is not None:
                where.append(f"{col}>=:{col}_lo")
                params[f"{col}_lo"] = lo
            if hi is not None:
                where.append(f"{col}<=:{col}_hi")
                params[f"{col}_hi"] = hi
        cols = ", ".join(_PAYLOAD_COLUMNS)
        with self._lock:
            rows = list(self._conn.execute(
                f"SELECT {cols}, rowid AS _rid FROM tasks_archive "
                f"WHERE {' AND '.join(where)} ORDER BY rowid LIMIT :lim",
                params,
            ))
            items = [(self._to_task(r), r["_rid"]) for r in rows]
            has_more = len(items) > limit
            items = items[:limit]
            return Page(
                items=[t for t, _ in items],
                limit=limit,
                next_cursor=items[-1][1] if has_more else None,
            )

    # ---------- 终态任务归档 ----------

    def archive_terminal(
        self,
        *,
        older_than: float | None = None,
        min_age_seconds: float = 86400.0,
        batch_size: int = 200,
        dry_run: bool = False,
    ) -> int:
        """把终结已久（SUCCEEDED/FAILED）的任务搬到归档表。

        - 仅归档终态且 ``updated_at <= now-min_age_seconds`` 的任务；
        - 任务行与其执行历史一起搬到 ``tasks_archive`` / ``task_events_archive``；
        - 归档后终态结论、最终产出与执行历史仍可通过 get/get_result/list_events 读回；
        - 分批短事务，每批独立提交，不持长事务，不挡新任务的领取与提交。
        返回本次归档的任务数（dry_run 时为预计数）。
        """
        now = self._now()
        cutoff = older_than if older_than is not None else now - min_age_seconds
        archived_total = 0
        with self._lock:
            while True:
                rows = list(self._conn.execute(
                    f"SELECT {', '.join(_PAYLOAD_COLUMNS)} FROM tasks "
                    "WHERE status IN ('succeeded','failed') AND updated_at<=? "
                    "ORDER BY updated_at, id LIMIT ?",
                    (cutoff, batch_size),
                ))
                if not rows:
                    break
                if dry_run:
                    return archived_total + len(rows)
                for row in rows:
                    self._archive_one(row, now)
                self._conn.commit()
                archived_total += len(rows)
                if len(rows) < batch_size:
                    break
        return archived_total

    def _archive_one(self, row: sqlite3.Row, now: float) -> None:
        task_cols = ", ".join(_PAYLOAD_COLUMNS)
        placeholders = ", ".join("?" for _ in _PAYLOAD_COLUMNS)
        values = [row[c] for c in _PAYLOAD_COLUMNS]
        # archived_at 位于 _PAYLOAD_COLUMNS 中，覆盖该列为当前归档时刻。
        values[_PAYLOAD_COLUMNS.index("archived_at")] = now
        self._conn.execute(
            f"INSERT OR REPLACE INTO tasks_archive ({task_cols}) "
            f"VALUES ({placeholders})",
            values,
        )
        events = list(self._conn.execute(
            "SELECT id, task_id, event, actor, lease_token, from_status, to_status, "
            "version, reason, created_at FROM task_events WHERE task_id=?",
            (row["id"],),
        ))
        self._conn.executemany(
            "INSERT OR REPLACE INTO task_events_archive (id, task_id, event, actor, "
            "lease_token, from_status, to_status, version, reason, created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            [tuple(e) for e in events],
        )
        # 归档事件只属于归档表：先取主表该任务的下一事件序号，直接写归档表，
        # 避免它同时留在主表导致读回重复。
        next_id = self._conn.execute(
            "SELECT COALESCE(MAX(id),0)+1 FROM task_events WHERE task_id=?",
            (row["id"],),
        ).fetchone()[0]
        terminal_status = TaskStatus(row["status"])
        self._conn.execute(
            "INSERT OR REPLACE INTO task_events_archive (id, task_id, event, actor, "
            "lease_token, from_status, to_status, version, reason, created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                next_id, row["id"], "archive", "ops", None,
                terminal_status.value, terminal_status.value, row["version"],
                f"终态任务归档 archived_at={now}", now,
            ),
        )
        self._conn.execute("DELETE FROM task_events WHERE task_id=?", (row["id"],))
        self._conn.execute("DELETE FROM tasks WHERE id=?", (row["id"],))
