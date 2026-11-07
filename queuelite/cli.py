"""Command line interface for queue-lite."""

from __future__ import annotations

import argparse
import importlib
import json
import sys
import time
from datetime import datetime, timezone
from typing import Any, Optional, Sequence

from .engine import InvalidState, LeaseLost, Queue
from .job import JOB_STATES

EXIT_OK = 0
EXIT_EMPTY = 1
EXIT_ERROR = 2
EXIT_LEASE_LOST = 4
EXIT_BAD_STATE = 5


def _iso(epoch: Optional[float]) -> Optional[str]:
    if epoch is None:
        return None
    return datetime.fromtimestamp(float(epoch), tz=timezone.utc).isoformat(timespec="seconds")


def _json_arg(value: Optional[str], field: str) -> Any:
    if value is None:
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{field} is not valid JSON: {exc}") from exc


def _queue(args: argparse.Namespace) -> Queue:
    return Queue(
        args.db,
        lease_seconds=args.lease,
        default_max_attempts=args.max_attempts,
        jitter=args.jitter,
    )


def _print_job(job, *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(job.to_dict(), indent=2, ensure_ascii=False))
        return
    print(
        f"#{job.id:<4} {job.state:<9} {job.queue:<10} attempts {job.attempts}/{job.max_attempts} "
        f"priority {job.priority:<3} run_at {_iso(job.run_at)}"
    )
    if job.payload:
        print(f"      payload {json.dumps(dict(job.payload), sort_keys=True)}")
    if job.last_error:
        print(f"      last_error {job.last_error}")
    if job.result is not None:
        print(f"      result {json.dumps(job.result, sort_keys=True)}")


def cmd_init(args: argparse.Namespace) -> int:
    queue = _queue(args)
    queue.init()
    stats = queue.stats()
    print(json.dumps({"db": args.db, "ready": stats.ready, "counts": stats.counts}, indent=2))
    queue.close()
    return EXIT_OK


def cmd_add(args: argparse.Namespace) -> int:
    payload = _json_arg(args.payload, "--payload")
    queue = _queue(args)
    try:
        result = queue.add(
            payload,
            queue=args.queue,
            priority=args.priority,
            delay=args.delay,
            max_attempts=args.job_max_attempts,
            unique_key=args.unique,
        )
    finally:
        queue.close()
    if args.json:
        print(json.dumps({"id": result.id, "duplicate": result.duplicate}, indent=2))
    else:
        print(f"job {result.id} {'already queued' if result.duplicate else 'added'}")
    return EXIT_OK


def cmd_take(args: argparse.Namespace) -> int:
    queue = _queue(args)
    try:
        jobs = queue.take(args.n, queue=args.queue, worker=args.worker)
    finally:
        queue.close()
    if not jobs:
        if args.json:
            print(json.dumps({"jobs": []}, indent=2))
        return EXIT_EMPTY
    if args.json:
        print(json.dumps({"jobs": [job.to_dict() for job in jobs]}, indent=2, ensure_ascii=False))
    else:
        for job in jobs:
            _print_job(job, as_json=False)
            print(f"      worker {job.worker} lease_expires {_iso(job.lease_expires_at)}")
    return EXIT_OK


def cmd_complete(args: argparse.Namespace) -> int:
    result = _json_arg(args.result, "--result")
    queue = _queue(args)
    try:
        job = queue.complete(args.id, args.worker, result)
    finally:
        queue.close()
    _print_job(job, as_json=args.json)
    return EXIT_OK


def cmd_fail(args: argparse.Namespace) -> int:
    queue = _queue(args)
    try:
        job = queue.fail(args.id, args.worker, args.error, retry_in=args.retry_in)
    finally:
        queue.close()
    _print_job(job, as_json=args.json)
    return EXIT_OK


def cmd_reap(args: argparse.Namespace) -> int:
    queue = _queue(args)
    try:
        result = queue.reap()
    finally:
        queue.close()
    if args.json:
        print(json.dumps({"recovered": list(result.recovered), "dead": list(result.dead)}, indent=2))
    else:
        print(f"recovered {len(result.recovered)}  dead {len(result.dead)}")
    return EXIT_OK


def cmd_list(args: argparse.Namespace) -> int:
    queue = _queue(args)
    try:
        jobs = queue.list(state=args.state, queue=args.queue, limit=args.limit)
    finally:
        queue.close()
    if args.json:
        print(json.dumps({"jobs": [job.to_dict() for job in jobs]}, indent=2, ensure_ascii=False))
        return EXIT_OK
    if not jobs:
        print("no jobs")
        return EXIT_OK
    for job in jobs:
        _print_job(job, as_json=False)
    return EXIT_OK


def cmd_stats(args: argparse.Namespace) -> int:
    queue = _queue(args)
    try:
        stats = queue.stats(queue=args.queue)
    finally:
        queue.close()
    payload = stats.to_dict()
    if args.json:
        print(json.dumps(payload, indent=2))
        return EXIT_OK
    print("  ".join(f"{state} {payload['counts'][state]}" for state in JOB_STATES))
    print(
        f"ready {payload['ready']}  delayed {payload['delayed']}  "
        f"expired leases {payload['expired_leases']}"
    )
    if payload["oldest_pending_age"] is not None:
        print(f"oldest pending {payload['oldest_pending_age']:.0f}s")
    return EXIT_OK


def cmd_cancel(args: argparse.Namespace) -> int:
    queue = _queue(args)
    try:
        job = queue.cancel(args.id)
    finally:
        queue.close()
    _print_job(job, as_json=args.json)
    return EXIT_OK


def cmd_requeue(args: argparse.Namespace) -> int:
    queue = _queue(args)
    try:
        job = queue.requeue(args.id)
    finally:
        queue.close()
    _print_job(job, as_json=args.json)
    return EXIT_OK


def cmd_purge(args: argparse.Namespace) -> int:
    queue = _queue(args)
    try:
        removed = queue.purge(older_than=args.older_than)
    finally:
        queue.close()
    if args.json:
        print(json.dumps({"deleted": removed}, indent=2))
    else:
        print(f"deleted {removed} job(s)")
    return EXIT_OK


def load_handler(spec: str):
    """Import ``module:function`` and return the callable."""
    if ":" not in spec:
        raise ValueError(f"handler must be module:function, got {spec!r}")
    module_name, _, attribute = spec.partition(":")
    module = importlib.import_module(module_name)
    try:
        handler = getattr(module, attribute)
    except AttributeError as exc:
        raise ValueError(f"{module_name} has no attribute {attribute!r}") from exc
    if not callable(handler):
        raise ValueError(f"{spec!r} is not callable")
    return handler


def cmd_work(args: argparse.Namespace) -> int:
    """Run ready jobs through a handler and exit, unless ``--follow`` is set."""
    handler = load_handler(args.handler)
    queue = _queue(args)
    processed = 0
    try:
        while True:
            if args.max_jobs and processed >= args.max_jobs:
                break
            if args.once and processed:
                break
            queue.reap()
            jobs = queue.take(1, queue=args.queue, worker=args.worker)
            if not jobs:
                if args.follow and not args.once:
                    time.sleep(args.poll)
                    continue
                break
            job = jobs[0]
            try:
                result = handler(dict(job.payload))
            except Exception as exc:  # noqa: BLE001 - a handler's failure is data
                failed = queue.fail(job.id, job.worker, f"{type(exc).__name__}: {exc}")
                if args.json:
                    print(json.dumps({"job": job.id, "state": failed.state,
                                      "error": failed.last_error}, indent=2))
                else:
                    print(f"#{job.id} failed: {failed.last_error} -> {failed.state}")
            else:
                done = queue.complete(job.id, job.worker, result)
                if args.json:
                    print(json.dumps({"job": job.id, "state": done.state}, indent=2))
                else:
                    print(f"#{job.id} done")
            processed += 1
    finally:
        queue.close()
    return EXIT_OK if processed else EXIT_EMPTY


def add_common(parser: argparse.ArgumentParser, *, top: bool) -> None:
    """Queue options, accepted before *or* after the subcommand.

    On subparsers the defaults are suppressed, so ``queuelite --db x stats`` does
    not have its ``--db`` overwritten by the subparser's own default.
    """

    def default(value):
        return value if top else argparse.SUPPRESS

    parser.add_argument("--db", default=default("jobs.db"), help="SQLite database file")
    parser.add_argument("--lease", type=float, default=default(300.0),
                        help="lease length in seconds")
    parser.add_argument("--max-attempts", type=int, default=default(3), dest="max_attempts",
                        help="default attempts per job")
    parser.add_argument("--jitter", type=float, default=default(0.2),
                        help="retry jitter, 0 to 1")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="queuelite",
        description="A durable job queue on SQLite (or Postgres) with leases and dead letters.",
    )
    add_common(parser, top=True)
    sub = parser.add_subparsers(dest="command", required=True)

    def command(name: str, help_text: str, func) -> argparse.ArgumentParser:
        child = sub.add_parser(name, help=help_text)
        add_common(child, top=False)
        child.set_defaults(func=func)
        return child

    command("init", "create the schema", cmd_init)

    add = command("add", "enqueue a job", cmd_add)
    add.add_argument("--queue", default="default")
    add.add_argument("--payload", default=None, help="JSON payload")
    add.add_argument("--priority", type=int, default=0, help="higher runs first")
    add.add_argument("--delay", type=float, default=0.0, help="seconds before it is ready")
    add.add_argument("--unique", default=None, help="dedupe key for active jobs")
    add.add_argument("--job-max-attempts", type=int, default=None)
    add.add_argument("--json", action="store_true")

    take = command("take", "claim ready jobs", cmd_take)
    take.add_argument("--queue", default="default")
    take.add_argument("--n", type=int, default=1)
    take.add_argument("--worker", default=None)
    take.add_argument("--json", action="store_true")

    complete = command("complete", "finish a job", cmd_complete)
    complete.add_argument("--id", type=int, required=True)
    complete.add_argument("--worker", required=True)
    complete.add_argument("--result", default=None, help="JSON result")
    complete.add_argument("--json", action="store_true")

    fail = command("fail", "record a failure", cmd_fail)
    fail.add_argument("--id", type=int, required=True)
    fail.add_argument("--worker", required=True)
    fail.add_argument("--error", required=True)
    fail.add_argument("--retry-in", type=float, default=None, dest="retry_in")
    fail.add_argument("--json", action="store_true")

    reap = command("reap", "recover expired leases", cmd_reap)
    reap.add_argument("--json", action="store_true")

    listing = command("list", "show jobs", cmd_list)
    listing.add_argument("--state", choices=list(JOB_STATES), default=None)
    listing.add_argument("--queue", default=None)
    listing.add_argument("--limit", type=int, default=20)
    listing.add_argument("--json", action="store_true")

    stats = command("stats", "counts by state", cmd_stats)
    stats.add_argument("--queue", default=None)
    stats.add_argument("--json", action="store_true")

    cancel = command("cancel", "withdraw a pending job", cmd_cancel)
    cancel.add_argument("--id", type=int, required=True)
    cancel.add_argument("--json", action="store_true")

    requeue = command("requeue", "retry a dead job", cmd_requeue)
    requeue.add_argument("--id", type=int, required=True)
    requeue.add_argument("--json", action="store_true")

    purge = command("purge", "delete terminal jobs", cmd_purge)
    purge.add_argument("--older-than", type=float, default=0.0, dest="older_than")
    purge.add_argument("--json", action="store_true")

    work = command("work", "run jobs through a handler", cmd_work)
    work.add_argument("--handler", required=True, help="module:function")
    work.add_argument("--queue", default="default")
    work.add_argument("--worker", default=None)
    work.add_argument("--max-jobs", type=int, default=0, dest="max_jobs")
    work.add_argument("--once", action="store_true", help="run at most one job")
    work.add_argument("--follow", action="store_true",
                      help="keep polling when the queue is empty until interrupted")
    work.add_argument("--poll", type=float, default=1.0)
    work.add_argument("--json", action="store_true")

    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return int(args.func(args))
    except LeaseLost as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_LEASE_LOST
    except InvalidState as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_BAD_STATE
    except (ValueError, ImportError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_ERROR


if __name__ == "__main__":
    raise SystemExit(main())
