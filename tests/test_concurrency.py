"""多消费者并发领取集成测试：同一任务绝不被重复执行。"""
from __future__ import annotations

import logging
import threading
from pathlib import Path

import pytest

from tasklease import TaskStore
from tasklease.errors import TaskRejected

from ._logutil import logger, log_task

TASK_COUNT = 30
CONSUMERS = 8


def test_concurrent_acquire_each_task_executed_once(tmp_path: Path) -> None:
    db = tmp_path / "pool.db"
    bootstrap = TaskStore(str(db))
    for i in range(TASK_COUNT):
        bootstrap.enqueue(f"job-{i:03d}", {"n": i}, max_attempts=1)
    bootstrap.close()

    stores = [TaskStore(str(db), default_lease_seconds=60) for _ in range(CONSUMERS)]
    executed: list[tuple[str, str]] = []  # (task_id, consumer)
    lock = threading.Lock()
    empty_rounds: dict[int, int] = {i: 0 for i in range(CONSUMERS)}
    stop = threading.Event()

    def worker(idx: int) -> None:
        st = stores[idx]
        consumer = f"consumer-{idx}"
        while not stop.is_set():
            task = st.acquire(consumer, lease_seconds=60)
            if task is None:
                empty_rounds[idx] += 1
                if sum(empty_rounds.values()) >= CONSUMERS * 3:
                    stop.set()
                stop.wait(0.001)
                continue
            with lock:
                empty_rounds[idx] = 0
            logger.info(
                "消费者 %s 领取到任务 %s（入队标识=%s, attempt=%s, 租约到期=%s）",
                consumer, task.id, task.idempotency_key, task.attempt,
                task.lease_expires_at,
            )
            try:
                st.complete(task.id, consumer, {"by": consumer},
                            expected_version=task.version)
                with lock:
                    executed.append((task.id, consumer))
            except TaskRejected as exc:
                # 并发下任何拒绝都必须可解释，不允许静默双执行
                logger.info("任务 %s 提交被拒: reason=%s", task.id, exc.reason.value)
                pytest.fail(f"不应出现提交拒绝: {exc.reason}")

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(CONSUMERS)]
    for t in threads:
        t.start()
    stop.wait(15)
    stop.set()
    for t in threads:
        t.join(timeout=5)

    task_ids = [tid for tid, _ in executed]
    assert len(task_ids) == TASK_COUNT
    assert len(set(task_ids)) == TASK_COUNT, "存在任务被重复执行！"

    # 每个任务恰好一个执行者
    owners = {tid: owner for tid, owner in executed}
    assert len(owners) == TASK_COUNT
    logging.getLogger("tasklease.tests").info(
        "并发执行完成：%s 个任务被 %s 个消费者各执行一次", TASK_COUNT, CONSUMERS
    )

    for st in stores:
        st.close()


def test_concurrent_acquire_with_lease_expiry_takeover(tmp_path: Path) -> None:
    """过期租约被其它进程/消费者接管：同一任务最终仍只成功一次。"""
    db = tmp_path / "takeover.db"
    bootstrap = TaskStore(str(db))
    bootstrap.enqueue("flaky-job", {}, max_attempts=3)
    bootstrap.close()

    outcome: dict[str, str] = {}
    fast_acquired = threading.Event()
    slow_done = threading.Event()

    def slow_worker() -> None:
        st = TaskStore(str(db), default_lease_seconds=1)
        t = st.acquire("slow", lease_seconds=1)
        if t is not None:
            # 模拟卡顿超过租约；等 fast 完成接管后再尝试提交
            fast_acquired.wait(5)
            try:
                st.complete(t.id, "slow", expected_version=t.version)
                outcome["slow"] = "succeeded"
            except TaskRejected as exc:
                outcome["slow"] = f"rejected:{exc.reason.value}"
            finally:
                slow_done.set()
        st.close()

    def fast_worker() -> None:
        import time
        st = TaskStore(str(db), default_lease_seconds=10)
        time.sleep(0.2)  # 等 slow 先领取
        # 轮询直到 slow 的租约过期并被 fast 接管
        for _ in range(40):
            t = st.acquire("fast", lease_seconds=10)
            if t is not None:
                break
            time.sleep(0.05)
        else:
            t = None
        if t is not None:
            assert t.lease_owner == "fast"
            log_task("fast 接管过期任务", t)
            fast_acquired.set()
            # 等持过期租约的 slow 提交被拒后，fast 再完成任务
            slow_done.wait(5)
            st.complete(t.id, "fast", expected_version=t.version)
            outcome["fast"] = "succeeded"
        st.close()

    t1 = threading.Thread(target=slow_worker)
    t2 = threading.Thread(target=fast_worker)
    t1.start(); t2.start()
    t1.join(); t2.join()

    assert outcome.get("fast") == "succeeded"
    # slow 持过期租约提交必被拒：未被接管前是 lease_expired，
    # 已被 fast 接管后是 lease_held_by_other，二者都保证旧租约不得提交结果。
    assert outcome["slow"] in (
        "rejected:lease_expired",
        "rejected:lease_held_by_other",
    ), outcome
    verifier = TaskStore(str(db))
    final = verifier.get_by_idempotency_key("flaky-job")
    assert final is not None and final.status.value == "succeeded"
    verifier.close()
