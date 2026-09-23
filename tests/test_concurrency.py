"""并发测试：多线程/多进程同时领取，同一任务只能被一个消费者执行。

日志打印入队标识、租约信息与状态流转判定依据。
"""

import logging
import multiprocessing as mp
import os
import threading

import pytest

from task_store.clock import FakeClock, SystemClock
from task_store.store import TaskStore
from task_store.types import (
    CompleteResult,
    EnqueueOutcome,
    LeaseResult,
    RejectReason,
    StateTransitionError,
)

log = logging.getLogger("task_store.tests.concurrency")


def test_concurrent_claim_single_winner_threads(tmp_path):
    db = str(tmp_path / "tasks.db")
    store = TaskStore(db, FakeClock(), default_lease_seconds=60)
    res = store.enqueue("concurrent-job-1", "p")
    assert res.outcome is EnqueueOutcome.CREATED
    winners: list[tuple[str, int]] = []
    barrier = threading.Barrier(16)

    def worker(idx: int):
        # 每个线程自己的连接（模拟多消费者/多进程各自的 store 句柄）
        s = TaskStore(db, FakeClock(), default_lease_seconds=60)
        owner = f"worker-{idx}"
        barrier.wait()
        outcome, lease = s.claim_next(owner)
        if lease is not None:
            winners.append((lease.owner, lease.fencing_token))
            log.info(
                "入队标识=%s 领取成功 owner=%s fencing=%d 结果=%s",
                "concurrent-job-1", lease.owner, lease.fencing_token, outcome.value,
            )
        else:
            log.info("owner=%s 领取结果=%s（任务已被他人持有）", owner, outcome.value)
        s.close()

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(winners) == 1, f"必须恰有一个消费者领到任务，实际: {winners}"
    task = store.get(res.task.id)
    assert task.status.value == "RUNNING"
    assert task.lease_owner == winners[0][0]
    store.close()


def test_concurrent_enqueue_same_key_single_row_threads(tmp_path):
    db = str(tmp_path / "tasks.db")

    def worker(counter):
        s = TaskStore(db, FakeClock())
        try:
            r = s.enqueue("same-key", "x")
            counter["created" if r.outcome is EnqueueOutcome.CREATED else "dedup"] += 1
        except StateTransitionError as e:
            counter["rejected:" + e.reason.value] += 1
        s.close()

    counter: dict[str, int] = {}
    lock = threading.Lock()
    threads = []
    for _ in range(12):
        def run():
            with lock:
                pass
            c = counter
            # 每个线程独立 store
            s = TaskStore(db, FakeClock())
            try:
                rr = s.enqueue("same-key", "x")
                key = "created" if rr.outcome is EnqueueOutcome.CREATED else "dedup"
            except StateTransitionError as e:
                key = "rejected:" + e.reason.value
            with lock:
                c[key] = c.get(key, 0) + 1
            s.close()
        threads.append(threading.Thread(target=run))
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert counter.get("created", 0) == 1
    rows = TaskStore(db, FakeClock()).list_tasks()
    assert len(rows) == 1


def _proc_worker(db: str, owner: str, q: "mp.Queue"):
    s = TaskStore(db, SystemClock(), default_lease_seconds=60)
    outcome, lease = s.claim_next(owner)
    if lease is not None:
        q.put((owner, lease.fencing_token, outcome.value))
    else:
        q.put((owner, None, outcome.value))
    s.close()


@pytest.mark.skipif(os.name == "nt", reason="multiprocessing on sqlite: posix only")
def test_concurrent_claim_multi_process(tmp_path):
    db = str(tmp_path / "tasks.db")
    store = TaskStore(db, SystemClock())
    store.enqueue("proc-job-1")
    store.close()

    ctx = mp.get_context("spawn")
    q: "mp.Queue" = ctx.Queue()
    procs = [
        ctx.Process(target=_proc_worker, args=(db, f"proc-{i}", q))
        for i in range(6)
    ]
    for p in procs:
        p.start()
    winners = []
    for _ in procs:
        owner, token, outcome = q.get()
        logging.info("进程 owner=%s token=%s 判定=%s", owner, token, outcome)
        if token is not None:
            winners.append((owner, token))
    for p in procs:
        p.join()
    assert len(winners) == 1, f"多进程下必须只有一个赢家: {winners}"
