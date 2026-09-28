"""本地运维命令行入口：异常运行任务的扫描/恢复与终态任务归档。

用法示例：
    python -m tasklease.cli --db tasklease.db recovery scan
    python -m tasklease.cli --db tasklease.db recovery run
    python -m tasklease.cli --db tasklease.db recovery run --requeue-corrupt
    python -m tasklease.cli --db tasklease.db recovery requeue <task_id>
    python -m tasklease.cli --db tasklease.db archive run --older-than 86400
"""
from __future__ import annotations

import argparse
import sys

from .archive import ArchiveService
from .recovery import RecoveryService
from .store import TaskStore


def build_parser() -> argparse.ArgumentParser:
    """构造运维命令行解析器（仅做参数解析，不触碰数据库）。"""
    parser = argparse.ArgumentParser(prog="tasklease", description="本地任务运维入口")
    parser.add_argument("--db", default="tasklease.db", help="SQLite 数据库路径")
    sub = parser.add_subparsers(dest="command", required=True)

    rec = sub.add_parser("recovery", help="异常运行任务分类与恢复")
    rec_sub = rec.add_subparsers(dest="action", required=True)
    rec_sub.add_parser("scan", help="只分类统计，不做任何改写")
    run_p = rec_sub.add_parser("run", help="恢复过期租约；--requeue-corrupt 时连同异常行放回")
    run_p.add_argument("--requeue-corrupt", action="store_true")
    run_p.add_argument("--limit", type=int, default=500)
    rq = rec_sub.add_parser("requeue", help="把隔离中的任务人工放回 PENDING")
    rq.add_argument("task_id")

    arc = sub.add_parser("archive", help="终态任务归档")
    arc_sub = arc.add_subparsers(dest="action", required=True)
    run_a = arc_sub.add_parser("run", help="归档终结已久的终态任务")
    run_a.add_argument("--older-than", type=float, default=None,
                       help="仅归档 updated_at 早于 now-older_than 秒的任务")
    run_a.add_argument("--limit", type=int, default=500)
    one = arc_sub.add_parser("one", help="归档指定任务")
    one.add_argument("task_id")

    return parser


def _print_report(title: str, report: object) -> None:
    print(f"== {title} ==")
    for field in (
        "scanned_running", "active", "expired", "corrupt",
        "recovered", "quarantined", "skipped",
        "archived", "already_archived", "skipped_non_terminal",
    ):
        if hasattr(report, field):
            print(f"  {field}: {getattr(report, field)}")
    for action in getattr(report, "actions", []):
        print("  -", " | ".join(str(x) for x in action))


def main(argv: list[str] | None = None) -> int:
    """执行运维命令，返回进程退出码。"""
    args = build_parser().parse_args(argv)
    with TaskStore(args.db) as store:
        if args.command == "recovery":
            svc = RecoveryService(store)
            if args.action == "scan":
                _print_report("RUNNING 任务分类（只读）", svc.scan())
            elif args.action == "run":
                report = svc.recover_all(
                    requeue_corrupt=args.requeue_corrupt, limit=args.limit
                )
                _print_report("异常运行任务恢复", report)
            elif args.action == "requeue":
                task = svc.requeue_quarantined(args.task_id)
                print(f"已放回: {task.id} status={task.status.value} "
                      f"version={task.version}")
        elif args.command == "archive":
            svc = ArchiveService(store)
            if args.action == "run":
                _print_report("终态任务归档", svc.archive_terminal(
                    older_than_seconds=args.older_than, limit=args.limit))
            elif args.action == "one":
                print(svc.archive_one(args.task_id).value)
    return 0


if __name__ == "__main__":
    sys.exit(main())
