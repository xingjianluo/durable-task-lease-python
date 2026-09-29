"""任务持久化存储：执行凭证、并发领取、租约、失败重试与乐观版本控制。

并发模型：进程内以一把可重入锁串行化会话访问；跨进程依赖 SQLite 的
写事务串行化与"条件 UPDATE ... WHERE"原子领取，保证同一任务任意时刻
只被一个消费者持有。所有状态变更都带版本号递增，过期版本写入被拒绝。

执行凭证（fence token）：每次 ``acquire`` 成功都会生成一个只属于本次执行的
``lease_token`` 并随回执交给消费者。complete/fail/heartbeat/release 都应
回传它；只有“当前有效租约 + 凭证匹配”才能写成功。租约一轮换（重新领取、
释放、恢复），旧凭证立即失效。
"""
from __future__ import annotations

import base64
import dataclasses
import hmac
import json
import sqlite3
import threading
import time
import uuid
from collections.abc import Callable, Iterable
from typing import Any

from . import schema
from .errors import (
    EnqueueOutcome,
    HeartbeatOutcome,
    LeaseHealth,
    RecoveryAction,
    RecoveryResult,
    ReleaseOutcome,
    RejectReason,
    TaskRejected,
)
from .models import (
    LeaseGrant,
    Page,
    ResultView,
    Task,
    TaskEvent,
    TaskStatus,
)

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

_TERMINAL_STATUSES = TaskStatus.terminal_values()
_ARCHIVABLE = ("succeeded", "failed")


@dataclasses.dataclass(frozen=True)
class RecoveryItem:
    task_id: str
    result: RecoveryResult
    health: LeaseHealth | None


@dataclasses.dataclass(frozen=True)
class RecoveryReport:
    """一次批量恢复的汇总：各类计数 + 逐条结果。可反复执行。"""

    scanned: int
    items: tuple[RecoveryItem, ...]

    @property
    def recovered(self) -> int:
        return sum(
            1
            for i in self.items
            if i.result
            in (RecoveryResult.REQUEUED, RecoveryResult.MARKED_MANUAL)
        )

    @property
    def skipped_held(self) -> int:
        return sum(1 for i in self.items if i.result is RecoveryResult.SKIPPED_HELD)


@dataclasses.dataclass(frozen=True)
class ArchiveReport:
    """一次归档的汇总：实际归档数与扫描候选数。可反复执行。"""

    archived: int
    scanned: int
    task_ids: tuple[str, ...]


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

    def _to_task(
        self,
        row: sqlite3.Row,
        *,
        archived: bool = False,
        archived_at: float | None = None,
    ) -> Task:
        data = dict(row)
        data["status"] = TaskStatus(data["status"])
        data["payload"] = json.loads(data.get("payload") or "{}")
        data.pop("result", None)
        data.pop("archived_flag", None)
        data.pop("archived_at", None)  # 归档表有此列；统一由显式参数回填
        data["archived"] = archived
        data["archived_at"] = archived_at
        return Task(**data)

    def _fetch_by_id(self, task_id: str) -> sqlite3.Row | None:
        cur = self._conn.execute(
            f"SELECT {', '.join(_PAYLOAD_COLUMNS)} FROM tasks WHERE id=?",
            (task_id,),
        )
        return cur.fetchone()

    def _fetch_archived_by_id(self, task_id: str) -> sqlite3.Row | None:
        cur = self._conn.execute(
            f"SELECT {', '.join(_PAYLOAD_COLUMNS)}, archived_at "
            "FROM tasks_archived WHERE id=?",
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

    def _new_token(self) -> str:
        return uuid.uuid4().hex

    def _sync_key(
        self,
        key: str,
        status: TaskStatus,
        *,
        archived: bool | int | None = None,
    ) -> None:
        if not key:
            return
        if archived is None:
            self._conn.execute(
                "UPDATE task_keys SET status=? WHERE idempotency_key=?",
                (status.value, key),
            )
        else:
            self._conn.execute(
                "UPDATE task_keys SET status=?, archived=? WHERE idempotency_key=?",
                (status.value, 1 if archived else 0, key),
            )

    def _log_event(
        self,
        task_id: str,
        event: str,
        actor: str | None,
        from_status: TaskStatus | None,
        to_status: TaskStatus | None,
        version: int,
        reason: str = "",
        token: str | None = None,
    ) -> None:
        self._conn.execute(
            "INSERT INTO task_events (task_id, event, actor, from_status, to_status, "
            "version, reason, created_at, lease_token) VALUES (?,?,?,?,?,?,?,?,?)",
            (
                task_id,
                event,
                actor,
                from_status.value if from_status else None,
                to_status.value if to_status else None,
                version,
                reason,
                self._now(),
                token,
            ),
        )

    # ---------- 查询 ----------

    def get(self, task_id: str, *, include_archived: bool = True) -> Task:
        """读取任务；默认连归档区一起查（archived=True 表示来自归档）。"""
        with self._lock:
            row = self._fetch_by_id(task_id)
            if row is not None:
                return self._to_task(row)
            if include_archived:
                arch = self._fetch_archived_by_id(task_id)
                if arch is not None:
                    return self._to_task(
                        arch, archived=True, archived_at=arch["archived_at"]
                    )
            raise TaskRejected(
                RejectReason.TASK_NOT_FOUND, f"task {task_id!r} 不存在"
            )

    def get_by_idempotency_key(
        self, key: str, *, include_archived: bool = True
    ) -> Task | None:
        with self._lock:
            row = self._fetch_by_key(key)
            if row is not None:
                return self._to_task(row)
            if include_archived and key:
                cur = self._conn.execute(
                    f"SELECT {', '.join(_PAYLOAD_COLUMNS)}, archived_at "
                    "FROM tasks_archived WHERE idempotency_key=?",
                    (key,),
                )
                arch = cur.fetchone()
                if arch is not None:
                    return self._to_task(
                        arch, archived=True, archived_at=arch["archived_at"]
                    )
            return None

    def get_result(self, task_id: str) -> ResultView:
        """读回终态结果：最终状态、产出、失败原因、尝试次数（含归档任务）。"""
        with self._lock:
            row = self._fetch_by_id(task_id)
            archived = False
            if row is None:
                row = self._fetch_archived_by_id(task_id)
                archived = True
            if row is None:
                raise TaskRejected(
                    RejectReason.TASK_NOT_FOUND, f"task {task_id!r} 不存在"
                )
            return ResultView(
                task_id=row["id"],
                idempotency_key=row["idempotency_key"],
                status=TaskStatus(row["status"]),
                result=json.loads(row["result"] or "{}"),
                last_error=row["last_error"],
                attempt=row["attempt"],
                max_attempts=row["max_attempts"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
                archived=archived,
            )

    def list_events(
        self,
        task_id: str | None = None,
        *,
        statuses: Iterable[TaskStatus | str] | None = None,
        actor: str | None = None,
        since: float | None = None,
        until: float | None = None,
        limit: int = 50,
        cursor: str | None = None,
        include_archived: bool = True,
    ) -> Page[TaskEvent]:
        """分页查询执行历史，支持按任务、状态、操作者、时间范围过滤。

        状态过滤同时匹配事件的 from/to 状态（流转进入或离开该状态都算命中）。
        """
        if limit < 1:
            raise ValueError("limit 必须 >= 1")
        limit = min(limit, 500)
        where: list[str] = []
        params: list[Any] = []
        if task_id is not None:
            where.append("e.task_id = ?")
            params.append(task_id)
        if statuses:
            vals = [
                s.value if isinstance(s, TaskStatus) else str(s) for s in statuses
            ]
            marks = ", ".join("?" for _ in vals)
            where.append(f"(e.to_status IN ({marks}) OR e.from_status IN ({marks}))")
            params.extend(vals)
            params.extend(vals)
        if actor is not None:
            where.append("e.actor = ?")
            params.append(actor)
        if since is not None:
            where.append("e.created_at >= ?")
            params.append(since)
        if until is not None:
            where.append("e.created_at <= ?")
            params.append(until)

        sources = ["SELECT *, 0 AS archived_flag FROM task_events"]
        if include_archived:
            sources.append(
                "SELECT id, task_id, event, actor, from_status, to_status, "
                "version, reason, created_at, lease_token, 1 AS archived_flag "
                "FROM task_events_archived"
            )
        union = " UNION ALL ".join(sources)
        sql = (
            f"WITH ev AS ({union}) "
            "SELECT * FROM ev AS e"
        )
        if where:
            sql += " WHERE " + " AND ".join(where)

        anchor_id, anchor_time, anchor_arch = None, None, None
        if cursor:
            try:
                raw = base64.urlsafe_b64decode(cursor.encode()).decode()
                t_s, id_s, a_s = raw.split(":")
                anchor_time, anchor_id, anchor_arch = float(t_s), int(id_s), int(a_s)
            except (ValueError, UnicodeDecodeError) as exc:
                raise ValueError("非法的分页 cursor") from exc
            sql += (
                " AND (e.created_at, e.id, e.archived_flag) > "
                "(?, ?, ?)" if where else
                " WHERE (e.created_at, e.id, e.archived_flag) > (?, ?, ?)"
            )
            params.extend([anchor_time, anchor_id, anchor_arch])

        sql += " ORDER BY e.created_at, e.id, e.archived_flag LIMIT ?"
        params.append(limit + 1)

        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()

        items = [
            TaskEvent(
                id=r["id"],
                task_id=r["task_id"],
                event=r["event"],
                actor=r["actor"],
                from_status=r["from_status"],
                to_status=r["to_status"],
                version=r["version"],
                reason=r["reason"] or "",
                created_at=r["created_at"],
                lease_token=r["lease_token"],
                archived=bool(r["archived_flag"]),
            )
            for r in rows[:limit]
        ]
        next_cursor = None
        if len(rows) > limit and items:
            last = items[-1]
            raw = f"{last.created_at}:{last.id}:{1 if last.archived else 0}"
            next_cursor = base64.urlsafe_b64encode(raw.encode()).decode()
        return Page(items=items, next_cursor=next_cursor)

    def list_tasks(
        self,
        *,
        statuses: Iterable[TaskStatus | str] | None = None,
        since: float | None = None,
        until: float | None = None,
        limit: int = 50,
        cursor: str | None = None,
        include_archived: bool = False,
    ) -> Page[Task]:
        """分页查询任务，支持按状态与创建时间范围过滤（可含归档区）。"""
        if limit < 1:
            raise ValueError("limit 必须 >= 1")
        limit = min(limit, 500)
        where: list[str] = []
        params: list[Any] = []
        if statuses:
            vals = [
                s.value if isinstance(s, TaskStatus) else str(s) for s in statuses
            ]
            where.append(f"t.status IN ({', '.join('?' for _ in vals)})")
            params.extend(vals)
        if since is not None:
            where.append("t.created_at >= ?")
            params.append(since)
        if until is not None:
            where.append("t.created_at <= ?")
            params.append(until)

        cols = ", ".join(_PAYLOAD_COLUMNS)
        sources = [f"SELECT {cols}, NULL AS archived_at, 0 AS archived_flag FROM tasks"]
        if include_archived:
            sources.append(
                f"SELECT {cols}, archived_at, 1 AS archived_flag FROM tasks_archived"
            )
        union = " UNION ALL ".join(sources)
        sql = f"WITH ta AS ({union}) SELECT * FROM ta AS t"
        if where:
            sql += " WHERE " + " AND ".join(where)

        anchor_id, anchor_time = None, None
        if cursor:
            try:
                raw = base64.urlsafe_b64decode(cursor.encode()).decode()
                t_s, id_s = raw.split(":")
                anchor_time, anchor_id = float(t_s), id_s
            except (ValueError, UnicodeDecodeError) as exc:
                raise ValueError("非法的分页 cursor") from exc
            clause = "(t.created_at, t.id) > (?, ?)"
            sql += f" AND {clause}" if where else f" WHERE {clause}"
            params.extend([anchor_time, anchor_id])

        sql += " ORDER BY t.created_at, t.id LIMIT ?"
        params.append(limit + 1)

        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()

        items = [
            self._to_task(
                r,
                archived=bool(r["archived_flag"]),
                archived_at=r["archived_at"],
            )
            for r in rows[:limit]
        ]
        next_cursor = None
        if len(rows) > limit and items:
            last = items[-1]
            raw = f"{last.created_at}:{last.id}"
            next_cursor = base64.urlsafe_b64encode(raw.encode()).decode()
        return Page(items=items, next_cursor=next_cursor)

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

    def _converge_existing_key(
        self, key_row: sqlite3.Row, key: str, now: float
    ) -> tuple[Task, EnqueueOutcome]:
        """命中 task_keys 既有结论：按结论所在位置/状态决定收敛还是拒绝。"""
        status = TaskStatus(key_row["status"])
        if key_row["archived"]:
            # 既有结论已归档：归档不改变终态语义，重复入队仍被挡住。
            raise TaskRejected(
                RejectReason.TASK_TERMINAL,
                f"key={key} 的任务已终结（{status.value}）并归档，"
                "既有结论保持不变，不得重复入队",
            )
        if status is TaskStatus.DEAD_LETTER:
            raise TaskRejected(
                RejectReason.TASK_DEAD_LETTER,
                f"key={key} 的任务已被标记为 dead_letter 等待人工处理，"
                "不得重复入队",
            )
        row = self._fetch_by_id(key_row["task_id"])
        # task_keys 与 tasks 理论上一致；万一查不到，按结论状态给终态拒绝。
        if row is None:
            raise TaskRejected(
                RejectReason.TASK_TERMINAL,
                f"key={key} 已有结论 {status.value}，不得重复入队",
            )
        self._reject_if_enqueue_blocked(row, now, key)
        self._log_event(
            row["id"], "enqueue.idempotent_hit", None,
            TaskStatus(row["status"]), TaskStatus(row["status"]),
            row["version"], f"幂等命中 key={key}",
        )
        self._conn.commit()
        return self._to_task(row), EnqueueOutcome.EXISTING

    def _enqueue_locked(
        self,
        key: str,
        payload: dict[str, Any],
        max_attempts: int,
        now: float,
    ) -> tuple[Task, EnqueueOutcome]:
        # 结论表是幂等权威：先看历史结论（含已归档）。
        key_row = self._conn.execute(
            "SELECT task_id, status, archived FROM task_keys WHERE idempotency_key=?",
            (key,),
        ).fetchone()
        if key_row is not None:
            return self._converge_existing_key(key_row, key, now)

        existing = self._fetch_by_key(key)
        if existing is not None:
            # 结论表缺行（异常老库）：回填后按既有任务判定。
            self._conn.execute(
                "INSERT OR IGNORE INTO task_keys(idempotency_key, task_id, status, "
                "archived) VALUES (?,?,?,0)",
                (key, existing["id"], existing["status"]),
            )
            self._reject_if_enqueue_blocked(existing, now, key)
            self._log_event(
                existing["id"], "enqueue.idempotent_hit", None,
                TaskStatus(existing["status"]), TaskStatus(existing["status"]),
                existing["version"], f"幂等命中 key={key}",
            )
            self._conn.commit()
            return self._to_task(existing), EnqueueOutcome.EXISTING

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
            self._conn.execute(
                "INSERT INTO task_keys(idempotency_key, task_id, status, archived) "
                "VALUES (?,?, 'pending', 0)",
                (key, task_id),
            )
            self._log_event(
                task_id, "enqueue.created", None, None, TaskStatus.PENDING, 0,
                f"新建任务 key={key}",
            )
            self._conn.commit()
        except sqlite3.IntegrityError:
            # 并发下唯一索引/结论表拦截：收敛到既有任务，按其当前状态判定。
            self._conn.rollback()
            key_row = self._conn.execute(
                "SELECT task_id, status, archived FROM task_keys "
                "WHERE idempotency_key=?",
                (key,),
            ).fetchone()
            if key_row is not None:
                return self._converge_existing_key(key_row, key, now)
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
        if status is TaskStatus.DEAD_LETTER:
            raise TaskRejected(
                RejectReason.TASK_DEAD_LETTER,
                f"key={key} 的任务为 dead_letter，等待人工处理，不得重复入队",
            )
        if status is TaskStatus.RUNNING:
            expires = row["lease_expires_at"]
            if expires is not None and expires > now and row["lease_owner"]:
                raise TaskRejected(
                    RejectReason.TASK_EXECUTING,
                    f"key={key} 的任务正在被 {row['lease_owner']} 执行，"
                    f"租约到期时间={expires}（当前 {now}），不得重复入队",
                )
            # RUNNING 但租约已过期/异常：任务将被重新领取/恢复，
            # 重复提交直接收敛到同一任务。

    # ---------- 并发领取 ----------

    def acquire_grant(
        self, consumer: str, lease_seconds: float | None = None
    ) -> LeaseGrant | None:
        """领取任务并返回执行凭证；无任务可领取时返回 None。"""
        if not consumer:
            raise ValueError("consumer 不能为空")
        ttl = lease_seconds if lease_seconds is not None else self.default_lease_seconds
        if ttl <= 0:
            raise ValueError("lease_seconds 必须 > 0")
        now = self._now()
        expires = now + ttl
        token = self._new_token()
        with self._lock:
            # 只领取“可领取 PENDING”或“租约字段齐全但已到期的 RUNNING”。
            # 租约信息异常（owner/到期时间缺失）的 RUNNING 不由领取流程静默
            # 接管，统一交给显式恢复入口处理。
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
                           AND lease_expires_at<=:now)
                    ORDER BY available_at, created_at, id
                    LIMIT 1
                )
                RETURNING id, version
                """,
                {
                    "owner": consumer,
                    "exp": expires,
                    "now": now,
                    "token": token,
                },
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
                f"ttl={ttl}s，执行凭证已轮换（旧凭证即刻失效）",
                token=token,
            )
            self._sync_key_from_row(task_id, TaskStatus.RUNNING)
            self._conn.commit()
            row = self._fetch_by_id(task_id)
            assert row is not None
            return LeaseGrant(task=self._to_task(row), token=token)

    def acquire(
        self, consumer: str, lease_seconds: float | None = None
    ) -> Task | None:
        """兼容旧签名：仅返回任务快照（新代码请用 acquire_grant 取凭证）。"""
        grant = self.acquire_grant(consumer, lease_seconds)
        return grant.task if grant is not None else None

    def _sync_key_from_row(self, task_id: str, status: TaskStatus) -> None:
        row = self._conn.execute(
            "SELECT idempotency_key FROM tasks WHERE id=?", (task_id,)
        ).fetchone()
        if row is not None and row["idempotency_key"]:
            self._conn.execute(
                "UPDATE task_keys SET status=? WHERE idempotency_key=?",
                (status.value, row["idempotency_key"]),
            )

    # ---------- 结果提交 ----------

    def _guard_lease(
        self,
        row: sqlite3.Row,
        consumer: str,
        now: float,
        expected_version: int | None,
        token: str | None = None,
    ) -> TaskStatus:
        """校验状态机位置、租约归属与有效期、执行凭证、客户端版本。

        拒绝顺序保证结论可区分：
        终态/未领取 → 租约信息异常 → 已被别人接管（owner 不同）
        → 租约已过期 → 凭证对不上 → 版本冲突。
        """
        status = TaskStatus(row["status"])
        if status is TaskStatus.DEAD_LETTER:
            raise TaskRejected(
                RejectReason.TASK_DEAD_LETTER,
                f"任务 {row['id']} 已标记为 dead_letter 等待人工处理，"
                "不能再提交结果",
            )
        if status in (TaskStatus.SUCCEEDED, TaskStatus.FAILED):
            raise TaskRejected(
                RejectReason.TASK_TERMINAL,
                f"任务 {row['id']} 已处于终态 {status.value}，状态流转被拒绝",
            )
        if status is not TaskStatus.RUNNING:
            raise TaskRejected(
                RejectReason.NOT_PENDING_OR_UNCLAIMED,
                f"任务 {row['id']} 当前为 {status.value}，尚未被领取，不能提交结果",
            )

        owner = row["lease_owner"]
        expires = row["lease_expires_at"]
        current_token = row["lease_token"]

        if owner is None or expires is None:
            raise TaskRejected(
                RejectReason.LEASE_MALFORMED,
                f"任务 {row['id']} 的租约信息异常"
                f"（lease_owner={owner!r}, lease_expires_at={expires!r}），"
                "请先通过恢复入口处理，不得提交结果",
            )
        if owner != consumer:
            raise TaskRejected(
                RejectReason.LEASE_HELD_BY_OTHER,
                f"任务 {row['id']} 由 {owner!r} 持有（消费者 {consumer!r}），"
                "执行权已被接管，旧执行的凭证不再有效",
            )
        if expires <= now:
            raise TaskRejected(
                RejectReason.LEASE_EXPIRED,
                f"任务 {row['id']} 的租约已于 {expires} 过期（当前 {now}），"
                "持过期租约不得提交结果",
            )
        if token is not None and (
            current_token is None or not hmac.compare_digest(current_token, token)
        ):
            # 同名消费者在租约轮换后重新领取：owner 仍是自己但凭证已换新，
            # 上一轮执行在此被明确挡住，无法覆盖新一轮产出。
            raise TaskRejected(
                RejectReason.LEASE_TOKEN_MISMATCH,
                f"任务 {row['id']} 的执行凭证与当前有效租约不符："
                f"消费者 {consumer!r} 本次提交使用的凭证已随租约轮换失效"
                f"（租约到期 {expires}，当前 {now}）",
            )
        if expected_version is not None and expected_version != row["version"]:
            raise TaskRejected(
                RejectReason.VERSION_CONFLICT,
                f"版本冲突：客户端持有版本 {expected_version}，当前版本 {row['version']}",
            )
        return status

    def _token_clause(self, token: str | None) -> tuple[str, str | None]:
        """生成条件 UPDATE 的凭证片段：带凭证时必须精确匹配当前租约。"""
        if token is None:
            return "", None
        return "AND lease_token IS NOT NULL AND lease_token=?", token

    def complete(
        self,
        task_id: str,
        consumer: str,
        result: dict[str, Any] | None = None,
        expected_version: int | None = None,
        token: str | None = None,
    ) -> Task:
        now = self._now()
        with self._lock:
            row = self._fetch_by_id(task_id)
            if row is None:
                raise TaskRejected(
                    RejectReason.TASK_NOT_FOUND, f"task {task_id!r} 不存在"
                )
            self._guard_lease(row, consumer, now, expected_version, token)
            body = json.dumps(result or {}, separators=(",", ":"), sort_keys=True)
            tok_sql, tok_val = self._token_clause(token)
            params: list[Any] = [body, now, task_id, consumer, row["version"], now]
            if tok_val is not None:
                params.append(tok_val)
            cur = self._conn.execute(
                "UPDATE tasks SET status='succeeded', lease_owner=NULL, "
                "lease_expires_at=NULL, lease_token=NULL, result=?, last_error=NULL, "
                "version=version+1, updated_at=? "
                "WHERE id=? AND status='running' AND lease_owner=? AND version=? "
                f"AND lease_expires_at>? {tok_sql}",
                params,
            )
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
                f"状态流转 running->succeeded，消费者={consumer}",
                token=token,
            )
            self._sync_key_from_row(task_id, TaskStatus.SUCCEEDED)
            self._conn.commit()
            fresh = self._fetch_by_id(task_id)
            assert fresh is not None
            return self._to_task(fresh)

    def fail(
        self,
        task_id: str,
        consumer: str,
        error: str = "",
        backoff_seconds: float | None = None,
        expected_version: int | None = None,
        token: str | None = None,
    ) -> Task:
        now = self._now()
        with self._lock:
            row = self._fetch_by_id(task_id)
            if row is None:
                raise TaskRejected(
                    RejectReason.TASK_NOT_FOUND, f"task {task_id!r} 不存在"
                )
            self._guard_lease(row, consumer, now, expected_version, token)

            # attempt 在领取时已 +1：首败 attempt=1。达到上限 -> 终态 FAILED。
            exhausted = row["attempt"] >= row["max_attempts"]
            tok_sql, tok_val = self._token_clause(token)
            if exhausted:
                params: list[Any] = [
                    error, now, task_id, consumer, row["version"], now
                ]
                if tok_val is not None:
                    params.append(tok_val)
                cur = self._conn.execute(
                    "UPDATE tasks SET status='failed', lease_owner=NULL, "
                    "lease_expires_at=NULL, lease_token=NULL, last_error=?, "
                    "version=version+1, updated_at=? "
                    "WHERE id=? AND status='running' AND lease_owner=? AND version=? "
                    f"AND lease_expires_at>? {tok_sql}",
                    params,
                )
                to_status, reason_text = (
                    TaskStatus.FAILED,
                    "重试次数耗尽，进入终态 failed",
                )
            else:
                if backoff_seconds is None:
                    backoff_seconds = self.backoff_base_seconds * 2 ** (
                        row["attempt"] - 1
                    )
                available_at = now + backoff_seconds
                params = [
                    available_at, error, now, task_id, consumer,
                    row["version"], now,
                ]
                if tok_val is not None:
                    params.append(tok_val)
                cur = self._conn.execute(
                    "UPDATE tasks SET status='pending', lease_owner=NULL, "
                    "lease_expires_at=NULL, lease_token=NULL, available_at=?, "
                    "last_error=?, version=version+1, updated_at=? "
                    "WHERE id=? AND status='running' AND lease_owner=? AND version=? "
                    f"AND lease_expires_at>? {tok_sql}",
                    params,
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
                new_version, reason_text + (f"，error={error}" if error else ""),
                token=token,
            )
            self._sync_key_from_row(task_id, to_status)
            self._conn.commit()
            fresh = self._fetch_by_id(task_id)
            assert fresh is not None
            return self._to_task(fresh)

    # ---------- 租约续期 / 释放 ----------

    def heartbeat(
        self,
        task_id: str,
        consumer: str,
        lease_seconds: float | None = None,
        token: str | None = None,
    ) -> HeartbeatOutcome:
        ttl = lease_seconds if lease_seconds is not None else self.default_lease_seconds
        now = self._now()
        with self._lock:
            row = self._fetch_by_id(task_id)
            if (
                row is None
                or TaskStatus(row["status"]) is not TaskStatus.RUNNING
                or row["lease_owner"] is None
                or row["lease_expires_at"] is None
                or row["lease_owner"] != consumer
            ):
                self._conn.commit()
                return HeartbeatOutcome.NOT_HELD
            if row["lease_expires_at"] <= now:
                outcome = HeartbeatOutcome.EXPIRED
            elif token is not None and (
                row["lease_token"] is None
                or not hmac.compare_digest(row["lease_token"], token)
            ):
                outcome = HeartbeatOutcome.INVALID_TOKEN
            else:
                tok_sql, tok_val = self._token_clause(token)
                params: list[Any] = [now + ttl, now, task_id, consumer, now]
                if tok_val is not None:
                    params.append(tok_val)
                cur = self._conn.execute(
                    "UPDATE tasks SET lease_expires_at=?, version=version+1, "
                    "updated_at=? WHERE id=? AND status='running' AND lease_owner=? "
                    f"AND lease_expires_at>? {tok_sql}",
                    params,
                )
                if cur.rowcount == 0:
                    self._conn.rollback()
                    return HeartbeatOutcome.EXPIRED
                self._log_event(
                    task_id, "heartbeat", consumer, TaskStatus.RUNNING,
                    TaskStatus.RUNNING, row["version"] + 1,
                    f"租约续期 lease_expires_at={now + ttl} ttl={ttl}s",
                    token=token,
                )
                outcome = HeartbeatOutcome.RENEWED
            self._conn.commit()
            return outcome

    def release(
        self,
        task_id: str,
        consumer: str,
        token: str | None = None,
    ) -> ReleaseOutcome:
        now = self._now()
        with self._lock:
            row = self._fetch_by_id(task_id)
            if (
                row is None
                or TaskStatus(row["status"]) is not TaskStatus.RUNNING
                or row["lease_owner"] is None
                or row["lease_expires_at"] is None
                or row["lease_owner"] != consumer
            ):
                self._conn.commit()
                return ReleaseOutcome.NOT_HELD
            if row["lease_expires_at"] <= now:
                # 租约已过期：不做任何状态改写，等待领取流程重新分配，归属判定明确。
                self._log_event(
                    task_id, "release.already_expired", consumer,
                    TaskStatus.RUNNING, TaskStatus.RUNNING, row["version"],
                    f"释放被拒：租约已于 {row['lease_expires_at']} 过期",
                )
                self._conn.commit()
                return ReleaseOutcome.ALREADY_EXPIRED
            if token is not None and (
                row["lease_token"] is None
                or not hmac.compare_digest(row["lease_token"], token)
            ):
                self._log_event(
                    task_id, "release.invalid_token", consumer,
                    TaskStatus.RUNNING, TaskStatus.RUNNING, row["version"],
                    "释放被拒：执行凭证与当前有效租约不符（租约可能已轮换）",
                )
                self._conn.commit()
                return ReleaseOutcome.INVALID_TOKEN

            tok_sql, tok_val = self._token_clause(token)
            params: list[Any] = [now, now, task_id, consumer, now]
            if tok_val is not None:
                params.append(tok_val)
            cur = self._conn.execute(
                "UPDATE tasks SET status='pending', lease_owner=NULL, "
                "lease_expires_at=NULL, lease_token=NULL, available_at=?, "
                "version=version+1, updated_at=? "
                "WHERE id=? AND status='running' AND lease_owner=? "
                f"AND lease_expires_at>? {tok_sql}",
                params,
            )
            if cur.rowcount == 0:
                self._conn.rollback()
                return ReleaseOutcome.ALREADY_EXPIRED
            self._log_event(
                task_id, "release", consumer, TaskStatus.RUNNING, TaskStatus.PENDING,
                row["version"] + 1, "主动释放 running->pending，任务立即可重新领取",
                token=token,
            )
            self._sync_key_from_row(task_id, TaskStatus.PENDING)
            self._conn.commit()
            return ReleaseOutcome.RELEASED

    # ---------- 异常运行任务的判定与恢复（运维入口） ----------

    def inspect_lease_health(self, task_id: str) -> LeaseHealth:
        """区分三类运行中任务：有效持有 / 租约过期 / 租约信息异常。"""
        with self._lock:
            row = self._fetch_by_id(task_id)
            if row is None:
                raise TaskRejected(
                    RejectReason.TASK_NOT_FOUND, f"task {task_id!r} 不存在"
                )
            return self._classify_running(row, self._now())

    @staticmethod
    def _classify_running(row: sqlite3.Row, now: float) -> LeaseHealth:
        status = TaskStatus(row["status"])
        if status is not TaskStatus.RUNNING:
            return LeaseHealth.NOT_RUNNING
        owner = row["lease_owner"]
        expires = row["lease_expires_at"]
        if owner is None or not owner or expires is None:
            return LeaseHealth.MALFORMED
        return LeaseHealth.HELD if expires > now else LeaseHealth.EXPIRED

    def recover_running(
        self,
        action: RecoveryAction | str = RecoveryAction.REQUEUE,
        *,
        task_ids: Iterable[str] | None = None,
        limit: int = 100,
    ) -> RecoveryReport:
        """恢复异常运行任务。

        - ``action=requeue``：过期/异常的 RUNNING 恢复为 PENDING（可重新领取）；
        - ``action=manual``：标记为 DEAD_LETTER（人工处理，不再被领取）。

        正在被有效持有的任务一律跳过；逐条小事务提交，可反复执行，
        不阻塞正常的领取与提交。
        """
        if isinstance(action, str):
            action = RecoveryAction(action)
        if limit < 1:
            raise ValueError("limit 必须 >= 1")
        now = self._now()
        items: list[RecoveryItem] = []
        with self._lock:
            if task_ids is None:
                rows = self._conn.execute(
                    "SELECT id FROM tasks WHERE status='running' "
                    "ORDER BY updated_at, id LIMIT ?",
                    (limit,),
                ).fetchall()
                targets = [r["id"] for r in rows]
            else:
                targets = list(task_ids)

            for tid in targets:
                row = self._fetch_by_id(tid)
                if row is None:
                    raise TaskRejected(
                        RejectReason.TASK_NOT_FOUND, f"task {tid!r} 不存在"
                    )
                health = self._classify_running(row, now)
                if health is LeaseHealth.NOT_RUNNING:
                    # 重复执行恢复时走到这里：任务已被恢复/终结，无额外副作用。
                    items.append(
                        RecoveryItem(tid, RecoveryResult.SKIPPED_NOT_RUNNING, health)
                    )
                    continue
                if health is LeaseHealth.HELD:
                    items.append(
                        RecoveryItem(tid, RecoveryResult.SKIPPED_HELD, health)
                    )
                    continue

                if action is RecoveryAction.MANUAL:
                    cur = self._conn.execute(
                        "UPDATE tasks SET status='dead_letter', lease_owner=NULL, "
                        "lease_expires_at=NULL, lease_token=NULL, "
                        "version=version+1, updated_at=? "
                        "WHERE id=? AND status='running' AND ("
                        "lease_owner IS NULL OR lease_owner='' OR lease_expires_at IS NULL "
                        "OR lease_expires_at<=?)",
                        (now, tid, now),
                    )
                    if cur.rowcount == 0:
                        self._conn.rollback()
                        items.append(
                            RecoveryItem(tid, RecoveryResult.SKIPPED_HELD, health)
                        )
                        continue
                    to_status = TaskStatus.DEAD_LETTER
                    result = RecoveryResult.MARKED_MANUAL
                    reason_text = (
                        f"异常运行任务标记人工处理（{health.value}），"
                        "running->dead_letter"
                    )
                    event = "recover.manual"
                else:
                    cur = self._conn.execute(
                        "UPDATE tasks SET status='pending', lease_owner=NULL, "
                        "lease_expires_at=NULL, lease_token=NULL, available_at=?, "
                        "version=version+1, updated_at=? "
                        "WHERE id=? AND status='running' AND ("
                        "lease_owner IS NULL OR lease_owner='' OR lease_expires_at IS NULL "
                        "OR lease_expires_at<=?)",
                        (now, now, tid, now),
                    )
                    if cur.rowcount == 0:
                        self._conn.rollback()
                        items.append(
                            RecoveryItem(tid, RecoveryResult.SKIPPED_HELD, health)
                        )
                        continue
                    to_status = TaskStatus.PENDING
                    result = RecoveryResult.REQUEUED
                    reason_text = (
                        f"异常运行任务恢复可领取（{health.value}），"
                        "running->pending，旧凭证全部失效"
                    )
                    event = "recover.requeue"
                self._log_event(
                    tid, event, "ops-recovery", TaskStatus.RUNNING, to_status,
                    row["version"] + 1, reason_text,
                )
                self._sync_key_from_row(tid, to_status)
                self._conn.commit()
                items.append(RecoveryItem(tid, result, health))

        return RecoveryReport(scanned=len(targets), items=tuple(items))

    # ---------- 终态归档 ----------

    def archive_terminal(
        self,
        *,
        older_than: float | None = None,
        limit: int = 100,
    ) -> ArchiveReport:
        """把终结已久的 SUCCEEDED/FAILED 任务搬到归档区。

        - 仅归档终态任务；DEAD_LETTER 属人工处理范围，不自动归档。
        - ``older_than`` 相对当前时间：终结后超过该秒数才归档（默认立即）。
        - 任务与其执行历史整体搬迁；归档后 get/get_result/list_* 仍可读。
        - 逐条小事务提交，归档期间正常领取/提交不被阻塞；重复执行无副作用。
        """
        if limit < 1:
            raise ValueError("limit 必须 >= 1")
        now = self._now()
        cutoff = now - older_than if older_than is not None else now
        archived_ids: list[str] = []
        with self._lock:
            candidates = self._conn.execute(
                "SELECT id FROM tasks WHERE status IN (?, ?) AND updated_at<=? "
                "ORDER BY updated_at, id LIMIT ?",
                (_ARCHIVABLE[0], _ARCHIVABLE[1], cutoff, limit),
            ).fetchall()
            for cand in candidates:
                tid = cand["id"]
                row = self._fetch_by_id(tid)
                if row is None or TaskStatus(row["status"]).value not in _ARCHIVABLE:
                    continue
                try:
                    self._conn.execute(
                        f"INSERT INTO tasks_archived "
                        f"({', '.join(_PAYLOAD_COLUMNS)}, archived_at) "
                        f"SELECT {', '.join(_PAYLOAD_COLUMNS)}, ? FROM tasks WHERE id=?",
                        (now, tid),
                    )
                    self._conn.execute(
                        "INSERT INTO task_events_archived "
                        "(id, task_id, event, actor, from_status, to_status, version, "
                        "reason, created_at, lease_token, archived_at) "
                        "SELECT id, task_id, event, actor, from_status, to_status, "
                        "version, reason, created_at, lease_token, ? "
                        "FROM task_events WHERE task_id=?",
                        (now, tid),
                    )
                    self._conn.execute(
                        "DELETE FROM task_events WHERE task_id=?", (tid,)
                    )
                    deleted = self._conn.execute(
                        "DELETE FROM tasks WHERE id=? AND status IN (?, ?)",
                        (tid, _ARCHIVABLE[0], _ARCHIVABLE[1]),
                    )
                    if deleted.rowcount == 0:
                        # 并发下状态在搬迁前被改写：整体回滚这一条，跳过。
                        self._conn.rollback()
                        continue
                    self._conn.execute(
                        "UPDATE task_keys SET archived=1 WHERE idempotency_key=? "
                        "AND task_id=?",
                        (row["idempotency_key"], tid),
                    )
                    self._conn.commit()
                    archived_ids.append(tid)
                except sqlite3.IntegrityError:
                    self._conn.rollback()
                    # 已归档过（重复执行）：视为无副作用的 no-op。
                    continue
        return ArchiveReport(
            archived=len(archived_ids),
            scanned=len(candidates),
            task_ids=tuple(archived_ids),
        )
