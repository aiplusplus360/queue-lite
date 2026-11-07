"""Workers: the poll loop that turns handlers into throughput.

The interesting part is not the loop. It is that the loop treats *itself* as
disposable: a worker that crashes holds nothing, because the only thing it ever
owned was a timestamp on a row. Its job comes back after the lease expires and
another worker finishes it.

    >>> from .engine import Queue
    >>> queue = Queue(":memory:", clock=lambda: 1000.0)
    >>> _ = queue.add({"n": 21})
    >>> worker = Worker(queue, {"double": lambda payload: payload["n"] * 2})
    >>> worker.run_once()
    1
    >>> queue.list(state="done")[0].result
    42
"""

from __future__ import annotations

import logging
import signal
import threading
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict, Mapping, Optional, Sequence

from .engine import InvalidState, LeaseLost, Queue
from .job import DEAD, Job

__all__ = ["Handler", "PermanentFailure", "RunStats", "UnregisteredTask", "Worker"]

Handler = Callable[[Mapping[str, Any]], Any]


class PermanentFailure(RuntimeError):
    """A handler error that retrying cannot fix; the job is dead-lettered at once."""


class UnregisteredTask(PermanentFailure):
    """The payload asked for a task this worker has no handler for."""


@dataclass
class RunStats:
    """What one worker did, as counters rather than log lines."""

    claimed: int = 0
    done: int = 0
    retried: int = 0
    dead: int = 0
    lost: int = 0
    reaped: int = 0

    def as_dict(self) -> Dict[str, int]:
        return {
            "claimed": self.claimed,
            "done": self.done,
            "retried": self.retried,
            "dead": self.dead,
            "lost": self.lost,
            "reaped": self.reaped,
        }


class _Heartbeat:
    """A background lease extender. ``stop`` also joins the thread, so a test's
    temporary database can be deleted without a race on Windows."""

    def __init__(self) -> None:
        self._event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    @property
    def stopping(self) -> bool:
        return self._event.is_set()

    def stop(self) -> None:
        self._event.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)


class Worker:
    """Claims jobs from one or more queue names and runs handlers.

    ``handlers`` maps a task name to a callable taking the job payload. A worker
    with a single handler accepts any job; with several, the payload's ``task``
    key selects one, and an unknown task is a permanent failure rather than a
    retry loop that never succeeds.
    """

    def __init__(
        self,
        queue: Queue,
        handlers: Optional[Mapping[str, Handler]] = None,
        *,
        queues: Sequence[str] = ("default",),
        concurrency: int = 1,
        poll_interval: float = 1.0,
        worker_id: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        if concurrency < 1:
            raise ValueError("concurrency must be >= 1")
        if poll_interval < 0:
            raise ValueError("poll_interval must be >= 0")
        if not queues:
            raise ValueError("at least one queue name is required")

        self.queue = queue
        self.handlers: Dict[str, Handler] = dict(handlers or {})
        self.queues = tuple(queues)
        self.concurrency = int(concurrency)
        self.poll_interval = float(poll_interval)
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.log = logger or logging.getLogger("queuelite.worker")
        self.stats = RunStats()
        self._stop = threading.Event()
        self._stats_lock = threading.Lock()

    # -- registration ------------------------------------------------------

    def register(self, name: str, handler: Handler) -> "Worker":
        if not name:
            raise ValueError("task name must not be empty")
        if not callable(handler):
            raise TypeError("handler must be callable")
        self.handlers[name] = handler
        return self

    def stop(self) -> None:
        """Ask the loop to finish the job in hand and exit."""
        self._stop.set()

    @property
    def stopping(self) -> bool:
        return self._stop.is_set()

    def install_signal_handlers(self) -> None:
        """Turn SIGINT/SIGTERM into a graceful stop. Main thread only."""
        if threading.current_thread() is not threading.main_thread():
            raise RuntimeError("signal handlers can only be installed in the main thread")
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                signal.signal(sig, self._on_signal)
            except (ValueError, OSError):  # pragma: no cover - platform specific
                continue

    def _on_signal(self, signum, frame) -> None:  # pragma: no cover - signal path
        self.log.info("received signal %s; finishing the current job", signum)
        self.stop()

    # -- single job --------------------------------------------------------

    def _resolve(self, payload: Mapping[str, Any]) -> Handler:
        if not self.handlers:
            raise PermanentFailure("this worker has no handlers registered")
        if len(self.handlers) == 1:
            return next(iter(self.handlers.values()))
        task = payload.get("task")
        if task is None:
            raise UnregisteredTask(
                f"payload has no 'task' key; expected one of {sorted(self.handlers)}"
            )
        try:
            return self.handlers[str(task)]
        except KeyError:
            raise UnregisteredTask(
                f"no handler for task {task!r}; registered: {sorted(self.handlers)}"
            ) from None

    def _process(self, queue: Queue, job: Job) -> None:
        with self._stats_lock:
            self.stats.claimed += 1
        assert job.worker is not None  # set by take()

        try:
            handler = self._resolve(job.payload)
        except PermanentFailure as exc:
            self._settle(queue, job, queue.kill, f"{type(exc).__name__}: {exc}")
            return

        beat = self._start_heartbeat(queue, job)
        try:
            result = handler(dict(job.payload))
        except PermanentFailure as exc:
            self._settle(queue, job, queue.kill, f"{type(exc).__name__}: {exc}")
        except Exception as exc:  # noqa: BLE001 - a handler's failure is data
            message = f"{type(exc).__name__}: {exc}"
            try:
                failed = queue.fail(job.id, job.worker, message)
            except (LeaseLost, InvalidState):
                self._count_lost(job)
            else:
                with self._stats_lock:
                    if failed.state == DEAD:
                        self.stats.dead += 1
                    else:
                        self.stats.retried += 1
                self.log.warning("job %s failed (%s); retrying later", job.id, message)
        else:
            try:
                queue.complete(job.id, job.worker, result)
            except (LeaseLost, InvalidState):
                self._count_lost(job)
            else:
                with self._stats_lock:
                    self.stats.done += 1
        finally:
            beat.stop()

    def _settle(self, queue: Queue, job: Job, action, error: str) -> None:
        """Dead-letter through ``action``, absorbing a lost lease into a stat."""
        try:
            action(job.id, job.worker or "", error)
        except (LeaseLost, InvalidState):
            self._count_lost(job)
            return
        with self._stats_lock:
            self.stats.dead += 1
        self.log.warning("job %s dead-lettered: %s", job.id, error)

    def _count_lost(self, job: Job) -> None:
        with self._stats_lock:
            self.stats.lost += 1
        self.log.warning("job %s was reclaimed by another worker; dropping our result", job.id)

    def _start_heartbeat(self, queue: Queue, job: Job) -> "_Heartbeat":
        """Extend the lease while a slow handler runs, on its own connection."""
        keeper = _Heartbeat()
        if queue.path == ":memory:":
            return keeper
        interval = max(0.5, queue.lease_seconds / 3.0)

        def beat() -> None:
            try:
                side = queue.fork()
            except (ValueError, OSError):  # pragma: no cover - defensive
                return
            try:
                while not keeper.stopping:
                    if keeper._event.wait(interval):
                        return
                    try:
                        side.heartbeat(job.id, job.worker or "", extend=queue.lease_seconds)
                    except (LeaseLost, InvalidState):
                        return
            finally:
                side.close()

        keeper._thread = threading.Thread(target=beat, name=f"lease-{job.id}", daemon=True)
        keeper._thread.start()
        return keeper

    # -- loops -------------------------------------------------------------

    def run_once(self, limit: int = 1, *, queue: Optional[Queue] = None,
                 worker_id: Optional[str] = None) -> int:
        """Process up to ``limit`` ready jobs across the configured queues."""
        if limit < 1:
            raise ValueError("limit must be >= 1")
        target = queue or self.queue
        token = worker_id or self.worker_id
        processed = 0
        for name in self.queues:
            if processed >= limit or self.stopping:
                break
            jobs = target.take(limit - processed, queue=name, worker=token)
            for job in jobs:
                self._process(target, job)
                processed += 1
        return processed

    def run_until_idle(self, max_jobs: int = 0, max_rounds: int = 1000) -> int:
        """Drain whatever is ready right now, then return. For cron-style runs."""
        processed = 0
        for _ in range(max_rounds):
            if self.stopping or (max_jobs and processed >= max_jobs):
                break
            limit = min(10, max_jobs - processed) if max_jobs else 10
            got = self.run_once(limit)
            if not got:
                break
            processed += got
        return processed

    def run_forever(self) -> RunStats:
        """Reap, claim, run, sleep. Exits when ``stop()`` is called."""
        reaped = self.queue.reap()
        with self._stats_lock:
            self.stats.reaped += len(reaped)
        if reaped:
            self.log.info("recovered %d expired lease(s)", len(reaped))

        if self.concurrency == 1:
            self._loop(self.queue)
        else:
            threads = []
            for index in range(self.concurrency - 1):
                side = self.queue.fork()
                thread = threading.Thread(
                    target=self._thread_main,
                    args=(side, f"{self.worker_id}-{index + 1}"),
                    name=f"queuelite-{index + 1}",
                    daemon=True,
                )
                thread.start()
                threads.append(thread)
            try:
                self._loop(self.queue)
            finally:
                self._stop.set()
                for thread in threads:
                    thread.join(timeout=5.0)
        return self.stats

    def _thread_main(self, queue: Queue, worker_id: str) -> None:
        try:
            self._loop(queue, worker_id)
        finally:
            queue.close()

    def _loop(self, queue: Queue, worker_id: Optional[str] = None) -> None:
        while not self._stop.is_set():
            try:
                if not self.run_once(1, queue=queue, worker_id=worker_id):
                    self._stop.wait(self.poll_interval)
            except (InvalidState, LeaseLost) as exc:  # pragma: no cover - defensive
                self.log.error("queue error: %s", exc)
                self._stop.wait(self.poll_interval)
