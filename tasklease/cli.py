"""本地运维命令行：异常 RUNNING 任务的发现与恢复、终态任务归档。

用法示例：

    python -m tasklease.cli --db tasklease.db stuck
    python -m tasklease.cli --db tasklease.db recover --dry-run
    python -m tasklease.cli --db tasklease.db recover
    python -m tasklease.cli --db tasklease.db recover --auto-requeue
    python -m tasklease.cli --db tasklease.db requeue <task_id>
    python -m tasklease.cli --db tasklease.db archive --min-age-seconds 86400

所有恢复/归档动作都是幂等的、分批短事务，可安全反复执行，
不会误伤仍被有效持有的任务，也不会卡住正常领取与提交。
"""
from __future__ import annotations

import argparse
import json
from typing import Sequence

from .errors import RunningLeaseState
from .store import TaskStore


def _task_brief(t) -> dict[str, object]:
    return {
        "id": t.id,
        "idempotency_key": t.idempotency_key,
        "status": t.status.value,
        "attempt": t.attempt,
        "lease_owner": t.lease_owner,
        "lease_expires_at": t.lease_expires_at,
        "updated_at": t.updated_at,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="tasklease", description=__doc__)
    parser.add_argument("--db", default="tasklease.db", help="SQLite 数据库路径")
    sub = parser.add_subparsers(dest="command", required=True)

    p_stuck = sub.add_parser("stuck", help="列出异常/过期 RUNNING 任务（三分类）")
    p_stuck.add_argument("--limit", type=int, default=500)

    p_rec = sub.add_parser("recover", help="恢复异常/过期 RUNNING 任务")
    p_rec.add_argument("--task-id", default=None, help="只处理指定任务")
    p_rec.add_argument("--batch-size", type=int, default=100)
    p_rec.add_argument("--dry-run", action="store_true", help="只预览，不写入")
    p_rec.add_argument(
        "--auto-requeue", action="store_true",
        help="缺字段异常任务也直接重新可领取（默认隔离为 quarantined 等待人工）",
    )

    p_rq = sub.add_parser("requeue", help="把隔离任务人工确认后放回可领取队列")
    p_rq.add_argument("task_id")

    p_arch = sub.add_parser("archive", help="归档终结已久的任务")
    p_arch.add_argument("--min-age-seconds", type=float, default=86400.0)
    p_arch.add_argument("--batch-size", type=int, default=200)
    p_arch.add_argument("--dry-run", action="store_true")

    args = parser.parse_args(argv)
    store = TaskStore(args.db)
    try:
        if args.command == "stuck":
            tasks = store.list_stuck_running(limit=args.limit)
            out = []
            for t in tasks:
                state = store.classify_running(t.id)
                out.append({"state": state.value, "task": _task_brief(t)})
            print(json.dumps({"stuck": out}, ensure_ascii=False, indent=2))

        elif args.command == "recover":
            results = store.recover_stuck(
                args.task_id,
                batch_size=args.batch_size,
                dry_run=args.dry_run,
                quarantine_corrupt=not args.auto_requeue,
            )
            payload = []
            for t, o in results:
                item = {"task_id": t.id, "outcome": o.value}
                # 仅在实际执行时附带当前分类；dry_run 的 outcome 是“将执行的动作”。
                if not args.dry_run and o.name == "SKIPPED_ACTIVE":
                    item["state"] = RunningLeaseState.ACTIVE.value
                payload.append(item)
            print(json.dumps(
                {"dry_run": args.dry_run, "results": payload},
                ensure_ascii=False, indent=2,
            ))

        elif args.command == "requeue":
            t = store.requeue_quarantined(args.task_id)
            print(json.dumps(_task_brief(t), ensure_ascii=False, indent=2))

        elif args.command == "archive":
            n = store.archive_terminal(
                min_age_seconds=args.min_age_seconds,
                batch_size=args.batch_size,
                dry_run=args.dry_run,
            )
            print(json.dumps(
                {"archived": n, "dry_run": args.dry_run},
                ensure_ascii=False,
            ))
    finally:
        store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
