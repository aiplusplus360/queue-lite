"""A tour of queue-lite: priorities, dedupe, retries, dead letters, lost leases.

Run it with ``python examples/demo.py``. The clock is fake, so nothing sleeps:
the demo takes about a millisecond and still shows a lease expiring.
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from queuelite import DEAD, DONE, PENDING, Queue, Worker  # noqa: E402


class Clock:
    def __init__(self, start: float = 1_700_000_000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def banner(title: str) -> None:
    print(f"\n{title}\n{'-' * len(title)}")


def main() -> int:
    clock = Clock()
    queue = Queue(":memory:", clock=clock, lease_seconds=30.0, jitter=0.0, backoff_base=2.0)

    banner("1. a producer enqueues work")
    urgent = queue.add({"task": "resize", "photo": "cover.jpg"}, priority=10,
                       unique_key="cover")
    normal = queue.add({"task": "resize", "photo": "avatar.jpg"})
    queue.add({"task": "email", "to": "a@example.com"}, delay=60)
    duplicate = queue.add({"task": "resize", "photo": "cover.jpg"}, unique_key="cover")

    print(f"urgent job      #{urgent.id}")
    print(f"normal job      #{normal.id}")
    print(f"delayed job is not ready yet; dedupe returned job #{duplicate.id} "
          f"(duplicate={duplicate.duplicate})")

    banner("2. a worker takes the highest priority job first")
    worker = Worker(queue, {
        "resize": lambda payload: {"pixels": len(payload["photo"]) * 100},
        "email": lambda payload: {"sent_to": payload["to"]},
    })
    processed = worker.run_until_idle()
    print(f"processed {processed} ready job(s); the delayed one is still pending")
    for job in queue.list(limit=10):
        print(f"  #{job.id} {job.state:<8} priority={job.priority:<3} run_in={job.run_at - clock.now:+.0f}s")

    banner("3. a failing job backs off instead of hammering the upstream")
    queue.add({"task": "flaky"}, max_attempts=3)
    attempts = []

    def flaky(payload):
        attempts.append(clock.now)
        if len(attempts) == 1:
            raise RuntimeError("upstream 503")
        return {"recovered_after": len(attempts)}

    worker = Worker(queue, {"flaky": flaky})
    worker.run_once()
    pending = queue.list(state=PENDING)
    retry = next(job for job in pending if job.last_error)
    print(f"attempt 1 failed: {retry.last_error!r}")
    print(f"next attempt scheduled in {retry.run_at - clock.now:.0f}s (attempts {retry.attempts}/3)")

    clock.advance(60)
    worker.run_until_idle()
    print(f"retry result: {queue.list(state=DONE)[-1].result}")

    banner("4. a crashed worker's lease expires and the job comes back")
    orphan = queue.add({"task": "resize", "photo": "never-finished.jpg"})
    claimed = queue.take()[0]
    print(f"worker {claimed.worker!r} claimed #{claimed.id} and then died")
    clock.advance(31)
    reap = queue.reap()
    print(f"reap recovered {reap.recovered}, dead-lettered {reap.dead}")
    recovered = queue.list(state=PENDING)
    print(f"#{orphan.id} is {recovered[0].state} again with attempts={recovered[0].attempts}")

    banner("5. an unknown task is dead-lettered immediately, not retried forever")
    doomed = queue.add({"task": "no-such-task"}, max_attempts=9)
    Worker(queue, {"resize": lambda payload: None, "email": lambda payload: None}) \
        .run_until_idle()
    dead = queue.get(doomed.id)
    print(f"#{dead.id} is {dead.state}: {dead.last_error}")

    banner("6. the whole queue in one table")
    stats = queue.stats()
    for state, count in stats.counts.items():
        print(f"  {state:<10} {count}")
    print(f"  ready      {stats.ready}")
    print(f"  delayed    {stats.delayed}")
    print(f"  expired    {stats.expired_leases}")

    assert dead.state == DEAD
    assert stats.counts[DONE] >= 3
    print("\nall demo assertions passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
