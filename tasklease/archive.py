"""终态任务归档：终结已久的 SUCCEEDED/FAILED 记录迁入独立归档表。"""
from __future__ import annotations

from typing import TYPE_CHECKING

from .errors import ArchiveOutcome, ArchiveReport
from .models import TaskStatus

if TYPE_CHECKING:
    from .store import TaskStore


class ArchiveService:
    """把已终结任务归档到 tasks_archive，归档不改变终态语义。

    - 仅归档 SUCCEEDED/FAILED；可要求终结时刻（updated_at）早于给定年龄。
    - 归档后 get/结果查询/幂等入队结论保持不变（透明读取归档表）。
    - 逐条小事务迁移（候选只读选取 + 每条独立提交），不阻塞新任务的领取
      与提交；重复执行幂等（已归档的行不再重复计数为新迁移）。
    """

    def __init__(self, store: TaskStore) -> None:  # type: ignore[name-defined]
        self._store = store

    def archive_one(self, task_id: str) -> ArchiveOutcome:
        """归档指定任务。非终态跳过；已归档返回 ALREADY_ARCHIVED。"""
        st = self._store
        with st._lock:
            row = st._fetch_by_id(task_id, include_archive=False)
            if row is None:
                # 主表不存在：可能已归档，也可能真的不存在。
                if st._is_archived(task_id):
                    return ArchiveOutcome.ALREADY_ARCHIVED
                return ArchiveOutcome.NOT_FOUND
            status = TaskStatus(row["status"])
            if status not in (TaskStatus.SUCCEEDED, TaskStatus.FAILED):
                return ArchiveOutcome.SKIPPED_NON_TERMINAL
            st.archive_row(row, st._now())
            return ArchiveOutcome.ARCHIVED

    def archive_terminal(
        self, *, older_than_seconds: float | None = None, limit: int = 500
    ) -> ArchiveReport:
        """批量归档终态任务。

        ``older_than_seconds`` 给定时只归档 ``updated_at <= now-age`` 的任务，
        给刚终结的任务留出观察窗口。返回逐条动作与汇总计数。
        """
        if older_than_seconds is not None and older_than_seconds < 0:
            raise ValueError("older_than_seconds 不能为负")
        if limit < 1:
            raise ValueError("limit 必须 >= 1")
        st = self._store
        report = ArchiveReport()
        now = st._now()
        candidates = st.archive_terminal_candidates(
            older_than_seconds=older_than_seconds, limit=limit, now=now
        )
        for row in candidates:
            # 再次确认该行是否此前已归档（候选来自主表，一般不会；
            # 真正重复执行时主表已无此行，因此根本不会出现在候选里）。
            if st.archive_row(row, now):
                report.archived += 1
                report.actions.append((row["id"], ArchiveOutcome.ARCHIVED))
        report.already_archived = 0
        report.skipped_non_terminal = 0
        return report
