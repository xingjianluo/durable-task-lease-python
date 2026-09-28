"""结果与执行历史查询：分页 + 按状态/时间范围筛选，归档数据透明可读。"""
from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable
from typing import TYPE_CHECKING

from .errors import RejectReason, TaskRejected
from .models import Task, TaskStatus
from .queries import Page, TaskEventRecord, TaskResult

if TYPE_CHECKING:
    from .store import TaskStore

_TASK_COLS = (
    "id", "idempotency_key", "payload", "status", "attempt", "max_attempts",
    "version", "lease_owner", "lease_expires_at", "lease_token", "available_at",
    "last_error", "result", "created_at", "updated_at",
)


def _validate_window(limit: int, offset: int) -> None:
    if limit < 1 or limit > 1000:
        raise ValueError("limit 必须在 1..1000 之间")
    if offset < 0:
        raise ValueError("offset 不能为负")


def _normalize_statuses(statuses: Iterable[str] | None) -> list[str]:
    out: list[str] = []
    for s in statuses or ():
        # 非法状态值直接报错，避免筛选条件静默不命中。
        out.append(TaskStatus(s).value)
    return out


class HistoryService:
    """只读查询服务：终态产出、事件历史、任务列表。

    所有方法均为只读，不会阻塞领取/提交；归档后的任务同样可查到最终状态与产出。
    """

    def __init__(self, store: TaskStore) -> None:  # type: ignore[name-defined]
        self._store = store

    # ---------- 执行产出 / 失败原因 ----------

    def get_result(self, task_id: str) -> TaskResult:
        """读回终态任务的执行产出与失败结论（主表或归档表均可）。"""
        with self._store._lock:
            row = self._store.result_row(task_id)
            if row is None:
                raise TaskRejected(
                    RejectReason.TASK_NOT_FOUND, f"task {task_id!r} 不存在"
                )
            archived = "archived_at" in row.keys()  # noqa: SIM118  # sqlite3.Row 不是 dict
            return TaskResult(
                task_id=row["id"],
                idempotency_key=row["idempotency_key"],
                status=row["status"],
                result=json.loads(row["result"] or "{}"),
                last_error=row["last_error"],
                attempt=row["attempt"],
                lease_owner=row["lease_owner"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
                archived=archived,
            )

    # ---------- 执行/租约历史事件 ----------

    def list_events(
        self,
        task_id: str | None = None,
        *,
        statuses: list[str] | None = None,
        start: float | None = None,
        end: float | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Page[TaskEventRecord]:
        """分页查询执行历史事件，可按任务、流转后状态、时间范围筛选。

        ``statuses`` 匹配事件的 ``to_status``（如 succeeded/failed/pending/running/
        quarantined）；时间范围作用于事件发生时刻 ``created_at``（闭区间）。
        事件不随任务归档而删除，因此归档任务的完整尝试过程仍可查。
        """
        _validate_window(limit, offset)
        wanted = _normalize_statuses(statuses)
        where: list[str] = []
        params: list[object] = []
        if task_id is not None:
            where.append("task_id=?")
            params.append(task_id)
        if wanted:
            where.append(f"to_status IN ({','.join('?' for _ in wanted)})")
            params.extend(wanted)
        if start is not None:
            where.append("created_at>=?")
            params.append(start)
        if end is not None:
            where.append("created_at<=?")
            params.append(end)
        clause = (" WHERE " + " AND ".join(where)) if where else ""
        with self._store._lock:
            total = self._store._conn.execute(
                f"SELECT COUNT(*) FROM task_events{clause}", params
            ).fetchone()[0]
            cur = self._store._conn.execute(
                "SELECT id, task_id, event, actor, from_status, to_status, "
                f"version, reason, created_at FROM task_events{clause} "
                "ORDER BY created_at, id LIMIT ? OFFSET ?",
                (*params, limit, offset),
            )
            items = [
                TaskEventRecord(
                    id=r["id"], task_id=r["task_id"], event=r["event"],
                    actor=r["actor"], from_status=r["from_status"],
                    to_status=r["to_status"], version=r["version"],
                    reason=r["reason"] or "", created_at=r["created_at"],
                )
                for r in cur.fetchall()
            ]
        return Page(items=items, total=total, limit=limit, offset=offset)

    # ---------- 任务列表 ----------

    def list_tasks(
        self,
        *,
        statuses: list[str] | None = None,
        start: float | None = None,
        end: float | None = None,
        limit: int = 50,
        offset: int = 0,
        include_archived: bool = False,
    ) -> Page[Task]:
        """分页查询任务，按状态与 ``updated_at`` 时间范围筛选。

        默认只查主表（含 PENDING/RUNNING/QUARANTINED/未归档终态）；
        ``include_archived=True`` 时合并 tasks_archive，按更新时间统一分页，
        归档记录的 ``Task.archived`` 标记为 True。
        """
        _validate_window(limit, offset)
        wanted = _normalize_statuses(statuses)
        where: list[str] = []
        params: list[object] = []
        if wanted:
            where.append(f"status IN ({','.join('?' for _ in wanted)})")
            params.extend(wanted)
        if start is not None:
            where.append("updated_at>=?")
            params.append(start)
        if end is not None:
            where.append("updated_at<=?")
            params.append(end)
        clause = (" WHERE " + " AND ".join(where)) if where else ""
        cols = ", ".join(_TASK_COLS)

        def rows_from(table: str, extra_col: str = "") -> list[sqlite3.Row]:
            select = f"SELECT {cols}{(', ' + extra_col) if extra_col else ''} FROM {table}{clause}"
            with self._store._lock:
                return list(self._store._conn.execute(select, params).fetchall())

        live = [(r, False) for r in rows_from("tasks")]
        archived: list[tuple[sqlite3.Row, bool]] = []
        if include_archived:
            archived = [(r, True) for r in rows_from("tasks_archive", "archived_at")]

        merged = sorted(live + archived, key=lambda p: (p[0]["updated_at"], p[0]["id"]))
        total = len(merged)
        page_rows = merged[offset:offset + limit]
        items = [self._store._to_task(r, archived=arc) for r, arc in page_rows]
        return Page(items=items, total=total, limit=limit, offset=offset)
