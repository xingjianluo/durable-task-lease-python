"""异常运行任务（租约信息不完整/缺失或已过期）的分类与幂等恢复。"""
from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

from .errors import RecoveryAction, RecoveryReport, RecoveryState
from .models import Task, TaskStatus

if TYPE_CHECKING:
    from .store import TaskStore


def _corrupt_detail(row: sqlite3.Row) -> str:
    missing = [
        name
        for name in ("lease_owner", "lease_expires_at", "lease_token")
        if row[name] is None
    ]
    return "缺失字段: " + ",".join(missing) if missing else "未知"


class RecoveryService:
    """运维入口：把卡住的 RUNNING 任务分类并恢复。

    - ACTIVE（有效持有）：绝不触碰；
    - EXPIRED（租约过期但信息完整）：恢复为 PENDING 可重新领取；
    - CORRUPT（租约信息异常，典型为旧版本升级后的运行行）：默认隔离为
      QUARANTINED 等待人工，也可按策略直接放回 PENDING。

    恢复动作幂等：重复执行不产生额外副作用，也不影响正常领取/提交。
    每个动作都是带条件的独立小事务，扫描本身只读且不持长锁。
    """

    def __init__(self, store: TaskStore) -> None:  # type: ignore[name-defined]
        self._store = store

    # ---------- 分类 ----------

    def classify(self, task: Task, now: float | None = None) -> RecoveryState:
        """判定一条 RUNNING 任务当前属于“有效持有/已过期/信息异常”哪一类。"""
        if task.status is not TaskStatus.RUNNING:
            raise ValueError("classify 只适用于 RUNNING 任务")
        if (
            task.lease_owner is None
            or task.lease_expires_at is None
            or task.lease_token is None
        ):
            return RecoveryState.CORRUPT
        cur = now if now is not None else self._store._now()
        if task.lease_expires_at <= cur:
            return RecoveryState.EXPIRED
        return RecoveryState.ACTIVE

    def _classify_row(self, row: sqlite3.Row, now: float) -> RecoveryState:
        if (
            row["lease_owner"] is None
            or row["lease_expires_at"] is None
            or row["lease_token"] is None
        ):
            return RecoveryState.CORRUPT
        return RecoveryState.EXPIRED if row["lease_expires_at"] <= now else RecoveryState.ACTIVE

    # ---------- 只读扫描 ----------

    def scan(self) -> RecoveryReport:
        """只分类统计，不做任何改写。可随时安全重复执行。"""
        report = RecoveryReport()
        now = self._store._now()
        for row in self._store.iter_running():
            state = self._classify_row(row, now)
            report.scanned_running += 1
            if state is RecoveryState.ACTIVE:
                report.active += 1
            elif state is RecoveryState.EXPIRED:
                report.expired += 1
            else:
                report.corrupt += 1
        return report

    # ---------- 恢复 ----------

    def recover_expired(self, *, limit: int = 500) -> RecoveryReport:
        """仅恢复“信息完整但租约已过期”的 RUNNING 任务。

        有效持有与信息异常的行都不会被本动作触碰。重复执行：第二次扫描时
        这些行已不在 RUNNING 集合，计数归零，无额外副作用。
        """
        report = RecoveryReport()
        now = self._store._now()
        processed = 0
        for row in self._store.iter_running():
            if processed >= limit:
                break
            state = self._classify_row(row, now)
            report.scanned_running += 1
            if state is RecoveryState.ACTIVE:
                report.active += 1
                report.skipped += 1
                report.actions.append(
                    (row["id"], RecoveryAction.SKIPPED_ACTIVE, "租约有效，跳过")
                )
            elif state is RecoveryState.EXPIRED:
                report.expired += 1
                processed += 1
                if self._store.requeue_expired_row(row, now):
                    report.recovered += 1
                    report.actions.append(
                        (row["id"], RecoveryAction.REQUEUED_EXPIRED,
                         f"过期={row['lease_expires_at']}")
                    )
                else:
                    # 扫描与写入之间已被正常领取/提交：不与之争抢。
                    report.skipped += 1
                    report.actions.append(
                        (row["id"], RecoveryAction.EXPIRED_ALREADY_RECOVERED,
                         "恢复前已被正常流程接管")
                    )
            else:
                # 信息异常行留给 recover_all 或人工，本方法绝不碰。
                report.corrupt += 1
                report.skipped += 1
                report.actions.append(
                    (row["id"], RecoveryAction.NOT_RUNNING,
                     "租约信息异常，需 recover_all/人工处理")
                )
        return report

    def recover_all(
        self, *, requeue_corrupt: bool = False, limit: int = 500
    ) -> RecoveryReport:
        """恢复过期租约；默认把信息异常行隔离，或按 ``requeue_corrupt`` 直接放回。

        - ACTIVE 永远跳过（恢复期间正常执行不受影响）；
        - EXPIRED -> PENDING；
        - CORRUPT -> QUARANTINED（默认）或 PENDING（显式 requeue_corrupt=True）。
        """
        report = RecoveryReport()
        now = self._store._now()
        processed = 0
        for row in self._store.iter_running():
            if processed >= limit:
                break
            state = self._classify_row(row, now)
            report.scanned_running += 1
            if state is RecoveryState.ACTIVE:
                report.active += 1
                report.skipped += 1
                report.actions.append(
                    (row["id"], RecoveryAction.SKIPPED_ACTIVE, "租约有效，跳过")
                )
                continue
            processed += 1
            if state is RecoveryState.EXPIRED:
                report.expired += 1
                if self._store.requeue_expired_row(row, now):
                    report.recovered += 1
                    report.actions.append(
                        (row["id"], RecoveryAction.REQUEUED_EXPIRED,
                         f"过期={row['lease_expires_at']}")
                    )
                else:
                    report.skipped += 1
                    report.actions.append(
                        (row["id"], RecoveryAction.EXPIRED_ALREADY_RECOVERED,
                         "恢复前已被正常流程接管")
                    )
                continue

            report.corrupt += 1
            detail = _corrupt_detail(row)
            if requeue_corrupt:
                if self._store.requeue_corrupt_row(row, detail, now):
                    report.recovered += 1
                    report.actions.append(
                        (row["id"], RecoveryAction.REQUEUED_CORRUPT, detail)
                    )
                else:
                    report.skipped += 1
                    report.actions.append(
                        (row["id"], RecoveryAction.CORRUPT_ALREADY_RECOVERED,
                                                         "恢复前已离开 RUNNING")
                    )
            else:
                if self._store.quarantine_corrupt_row(row, detail):
                    report.quarantined += 1
                    report.actions.append(
                        (row["id"], RecoveryAction.QUARANTINED_CORRUPT, detail)
                    )
                else:
                    report.skipped += 1
                    report.actions.append(
                        (row["id"], RecoveryAction.CORRUPT_ALREADY_QUARANTINED,
                         "隔离前已离开 RUNNING")
                    )
        return report

    def requeue_quarantined(
        self, task_id: str, *, expected_version: int | None = None
    ) -> Task:
        """人工把隔离中的任务放回 PENDING，重新进入可领取队列。"""
        return self._store.requeue_quarantined(
            task_id, expected_version=expected_version
        )
