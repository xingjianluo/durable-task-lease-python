"""结果/历史查询的数据传输对象与分页容器。"""
from __future__ import annotations

import dataclasses
from typing import Any


@dataclasses.dataclass(frozen=True)
class Page[T]:
    """偏移分页结果。"""

    items: list[T]
    total: int
    limit: int
    offset: int

    @property
    def next_offset(self) -> int | None:
        nxt = self.offset + self.limit
        return nxt if nxt < self.total else None


@dataclasses.dataclass(frozen=True)
class TaskResult:
    """终态任务的执行产出与失败结论。"""

    task_id: str
    idempotency_key: str
    status: str
    result: dict[str, Any]
    last_error: str | None
    attempt: int
    lease_owner: str | None
    created_at: float
    updated_at: float
    archived: bool


@dataclasses.dataclass(frozen=True)
class TaskEventRecord:
    """一条租约/状态流转审计事件。"""

    id: int
    task_id: str
    event: str
    actor: str | None
    from_status: str | None
    to_status: str | None
    version: int
    reason: str
    created_at: float
