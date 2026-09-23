"""测试日志工具：打印入队标识、租约信息与状态流转判定依据。"""
from __future__ import annotations

import logging
import sqlite3

logger = logging.getLogger("tasklease.tests")


def log_task(label: str, task: object) -> None:
    logger.info(
        "[%s] 入队标识=%r 任务id=%s 状态=%s attempt=%s/%s 版本=%s "
        "租约持有者=%s 租约到期=%s 可领取时刻=%s",
        label,
        getattr(task, "idempotency_key", None),
        getattr(task, "id", None),
        getattr(getattr(task, "status", None), "value", task),
        getattr(task, "attempt", None),
        getattr(task, "max_attempts", None),
        getattr(task, "version", None),
        getattr(task, "lease_owner", None),
        getattr(task, "lease_expires_at", None),
        getattr(task, "available_at", None),
    )


def log_events(conn: sqlite3.Connection, task_id: str | None = None) -> None:
    sql = (
        "SELECT event, actor, from_status, to_status, version, reason, created_at "
        "FROM task_events"
    )
    params: tuple[object, ...] = ()
    if task_id is not None:
        sql += " WHERE task_id=?"
        params = (task_id,)
    sql += " ORDER BY id"
    for event, actor, frm, to, ver, reason, at in conn.execute(sql, params):
        logger.info(
            "状态流转判定依据: %s %s: %s->%s version=%s actor=%s | %s",
            at,
            event,
            frm,
            to,
            ver,
            actor,
            reason,
        )
