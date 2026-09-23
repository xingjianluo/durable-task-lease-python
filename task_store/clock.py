"""时间源抽象，便于测试中精确控制租约到期与退避。"""

from __future__ import annotations

import time
from collections import defaultdict
from typing import Protocol


class Clock(Protocol):
    def now(self) -> float:
        """返回当前单调时间（秒）。"""
        ...


class SystemClock:
    """生产环境使用的时间源（time.monotonic，不受系统时钟回拨影响）。"""

    def now(self) -> float:
        return time.monotonic()


class FakeClock:
    """测试用可控时间源，支持手动推进。"""

    def __init__(self, start: float = 1000.0):
        self._now = start

    def now(self) -> float:
        return self._now

    def advance(self, seconds: float) -> float:
        self._now += seconds
        return self._now
