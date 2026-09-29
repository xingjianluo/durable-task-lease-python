"""测试公共夹具：可控时钟 + 带操作日志的存储。"""
from __future__ import annotations

import logging
import threading

import pytest

from tasklease.store import TaskStore


class FakeClock:
    """可手动推进的时钟，默认从一个较大的整数值开始。"""

    def __init__(self, start: float = 1_000_000.0) -> None:
        self.now = start
        self._lock = threading.Lock()

    def __call__(self) -> float:
        with self._lock:
            return self.now

    def advance(self, seconds: float) -> None:
        with self._lock:
            self.now += seconds


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def store(clock: FakeClock) -> TaskStore:
    st = TaskStore(
        ":memory:",
        default_lease_seconds=10.0,
        backoff_base_seconds=1.0,
        clock=clock,
    )
    yield st
    st.close()


def claim(
    st: TaskStore, consumer: str, lease_seconds: float | None = None
) -> tuple[object, str]:
    """测试辅助：领取并断言成功，返回 (task, lease_token)。"""
    claimed = st.acquire(consumer, lease_seconds=lease_seconds)
    assert claimed is not None, f"消费者 {consumer} 未领取到任务"
    return claimed


# 单测日志：打印入队标识、租约信息与状态流转判定依据。
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
