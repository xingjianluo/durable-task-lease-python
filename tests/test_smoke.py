"""骨架冒烟测试：仅验证包可导入。"""

import task_store  # noqa: F401
from task_store.types import RejectReason, TaskStatus


def test_imports():
    assert TaskStatus.PENDING.value == "PENDING"
    assert RejectReason.LEASE_EXPIRED.value == "LEASE_EXPIRED"
