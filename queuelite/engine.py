"""The queue engine: claim, lease, complete, retry, reap.

Every write is one SQL statement or one transaction. There is no in-memory job
list to drift out of sync with the database, because the in-memory list is
exactly how a queue ends up processing the same job twice after a restart.
"""

from __future__ import annotations

import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Mapping, NoReturn, Optional, Sequence, Tuple

from . import backoff as backoff_mod
from .job import (
    CANCELLED,
    DEAD,
    DONE,
    JOB_STATES,
    PENDING,
    RUNNING,
    Job,
    encode_payload,
)
from .schema import INDEXES, SCHEMA

__all__ = [
    "AddResult",
    "InvalidState",
    "LeaseLost",
    "Queue",
    "ReapResult",
    "Stats",
]

FIELDS = (
    "id, queue, payload, state, priority, attempts, max_attempts, run_at, "
    "lease_expires_at, worker, last_error, unique_key, result, created_at, updated_at"
)


class LeaseLost(RuntimeError):
    """The worker asked to finish a job it no longer holds the lease on."""


class InvalidState(RuntimeError):
    """The job exists but the transition is not legal from its current state."""


@dataclass(frozen=True)
class AddResult:
    """``duplicate`` is True when a unique key matched an active job."""

    id: int
    duplicate: bool = False


@dataclass(frozen=True)
class ReapResult:
    """``recovered`` went back to pending, ``dead`` exhausted its attempts."""

    recovered: Tuple[int, ...] = ()
    dead: Tuple[int, ...] = ()

    def __len__(self) -> int:
        return len(self.recovered) + len(self.dead)


@dataclass(frozen=True)
class Stats:
    counts: Dict[str, int]
    ready: int
    delayed: int
    expired_leases: int
    oldest_pending_age: Optional[float]

    def to_dict(self) -> dict:
        return {
            "counts": dict(self.counts),
            "ready": self.ready,
            "delayed": self.delayed,
            "expired_leases": self.expired_leases,
            "oldest_pending_age": self.oldest_pending_age,
        }


class Queue:
    """A durable queue over one SQLite database (or a Postgres one, same SQL).

    >>> queue = Queue(":memory:", clock=lambda: 1000.0)
    >>> queue.add({"task": "resize"}, queue="media").id
    1
    >>> job = queue.take(queue="media")[0]
    >>> (job.state, job.attempts, job.worker is not None)
    ('running', 1, True)
    >>> queue.complete(job.id, job.worker, result={"ok": True}).state
    'done'
    """

    def __init__(
        self,
        path: str = ":memory:",
        *,
        clock=time.time,
        lease_seconds: float = 300.0,
        default_max_attempts: int = 3,
        backoff_base: float = 2.0,
        backoff_cap: float = 300.0,
        jitter: float = 0.2,
        rng=None,
        connection: Optional[sqlite3.Connection] = None,
    ) -> None:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be > 0")
        if default_max_attempts < 1:
            raise ValueError("default_max_attempts must be >= 1")

        self.path = path
        self.clock = clock
        self.lease_seconds = float(lease_seconds)
        self.default_max_attempts = int(default_max_attempts)
        self.backoff_base = float(backoff_base)
        self.backoff_cap = float(backoff_cap)
        self.jitter = float(jitter)
        self.rng = rng

        self._conn = connection or sqlite3.connect(
            path, isolation_level=None, timeout=30.0, check_same_thread=False
        )
        self._lock = threading.RLock()
        self._conn.row_factory = sqlite3.Row
        if path != ":memory:":
            self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=30000")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self.init()

    # -- lifecycle ---------------------------------------------------------

    def init(self) -> None:
        """Create the table and indexes if they are missing. Idempotent."""
        with self._lock:
            self._conn.executescript(SCHEMA + INDEXES)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def fork(self, **overrides: Any) -> "Queue":
        """A second connection to the same database, for another thread or process.

        SQLite connections are not thread-safe objects, but the database is. Each
        worker thread gets its own ``Queue`` so no two threads share a cursor.
        """
        if self.path == ":memory:":
            raise ValueError(
                "in-memory queues cannot be forked: each SQLite connection gets its own "
                "database. Use a file path for multi-threaded work."
            )
        options: Dict[str, Any] = {
            "clock": self.clock,
            "lease_seconds": self.lease_seconds,
            "default_max_attempts": self.default_max_attempts,
            "backoff_base": self.backoff_base,
            "backoff_cap": self.backoff_cap,
            "jitter": self.jitter,
            "rng": self.rng,
        }
        options.update(overrides)
        return Queue(self.path, **options)

    def __enter__(self) -> "Queue":
        return self

    def __exit__(self, *_exc) -> None:
        self.close()

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Cursor]:
        """An immediate transaction: the write lock is taken before the read.

        This is the difference between "check the row is free, then take it" and
        "take the row, having checked it is free" -- with two workers, only the
        second is correct.

        The connection is shared across threads (``check_same_thread=False``) but
        guarded by a lock: one SQLite connection cannot run two statements at
        once, and a half-applied transaction is not a state anyone should see.
        """
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute("BEGIN IMMEDIATE")
            try:
                yield cursor
            except BaseException:
                cursor.execute("ROLLBACK")
                raise
            cursor.execute("COMMIT")

    # -- enqueue -----------------------------------------------------------

    def add(
        self,
        payload: Any = None,
        *,
        queue: str = "default",
        priority: int = 0,
        delay: float = 0.0,
        max_attempts: Optional[int] = None,
        unique_key: Optional[str] = None,
    ) -> AddResult:
        """Enqueue a job. Returns the existing id when ``unique_key`` is active.

        Deduplication is scoped to *active* work: once the previous job is done,
        dead or cancelled, the same key can be enqueued again.
        """
        if not queue:
            raise ValueError("queue must not be empty")
        if delay < 0:
            raise ValueError("delay must be >= 0")
        attempts_allowed = self.default_max_attempts if max_attempts is None else int(max_attempts)
        if attempts_allowed < 1:
            raise ValueError("max_attempts must be >= 1")

        now = self.clock()
        with self._tx() as cursor:
            if unique_key:
                row = cursor.execute(
                    "SELECT id FROM jobs WHERE unique_key = ? AND state IN (?, ?) "
                    "ORDER BY id LIMIT 1",
                    (unique_key, PENDING, RUNNING),
                ).fetchone()
                if row is not None:
                    return AddResult(int(row["id"]), duplicate=True)
            try:
                cursor.execute(
                    "INSERT INTO jobs (queue, payload, state, priority, attempts, max_attempts, "
                    "run_at, created_at, updated_at, unique_key) "
                    "VALUES (?, ?, ?, ?, 0, ?, ?, ?, ?, ?)",
                    (
                        queue,
                        encode_payload(payload),
                        PENDING,
                        int(priority),
                        attempts_allowed,
                        now + float(delay),
                        now,
                        now,
                        unique_key,
                    ),
                )
            except sqlite3.IntegrityError:
                # Two enqueues raced; the unique index caught it. Report the winner.
                if unique_key:
                    row = cursor.execute(
                        "SELECT id FROM jobs WHERE unique_key = ? AND state IN (?, ?) "
                        "ORDER BY id LIMIT 1",
                        (unique_key, PENDING, RUNNING),
                    ).fetchone()
                    if row is not None:
                        return AddResult(int(row["id"]), duplicate=True)
                raise
            return AddResult(int(cursor.lastrowid), duplicate=False)

    # -- claim -------------------------------------------------------------

    def take(
        self,
        limit: int = 1,
        *,
        queue: str = "default",
        worker: Optional[str] = None,
    ) -> List[Job]:
        """Claim up to ``limit`` ready jobs, newest priority first.

        Highest ``priority`` first, then earliest ``run_at``, then lowest id, so
        the order is total and reproducible.
        """
        if limit < 1:
            raise ValueError("limit must be >= 1")
        token = worker or f"{uuid.uuid4().hex[:12]}"
        now = self.clock()
        claimed: List[int] = []

        with self._tx() as cursor:
            rows = cursor.execute(
                f"SELECT {FIELDS} FROM jobs "
                "WHERE queue = ? AND state = ? AND run_at <= ? "
                "ORDER BY priority DESC, run_at ASC, id ASC LIMIT ?",
                (queue, PENDING, now, limit),
            ).fetchall()
            for row in rows:
                attempts = int(row["attempts"]) + 1
                cursor.execute(
                    "UPDATE jobs SET state = ?, worker = ?, lease_expires_at = ?, attempts = ?, "
                    "updated_at = ? WHERE id = ? AND state = ?",
                    (RUNNING, token, now + self.lease_seconds, attempts, now, row["id"], PENDING),
                )
                if cursor.rowcount == 1:
                    claimed.append(int(row["id"]))

        return [self.get(job_id) for job_id in claimed]  # type: ignore[list-item]

    def heartbeat(self, job_id: int, worker: str, *, extend: Optional[float] = None) -> Job:
        """Extend a lease. Long jobs call this to stay alive."""
        now = self.clock()
        lease = self.lease_seconds if extend is None else float(extend)
        if lease <= 0:
            raise ValueError("extend must be > 0")
        with self._tx() as cursor:
            cursor.execute(
                "UPDATE jobs SET lease_expires_at = ?, updated_at = ? "
                "WHERE id = ? AND state = ? AND worker = ?",
                (now + lease, now, job_id, RUNNING, worker),
            )
            if cursor.rowcount != 1:
                raise LeaseLost(f"job {job_id} is not leased to {worker!r}")
        return self.get(job_id)  # type: ignore[return-value]

    # -- finish ------------------------------------------------------------

    def complete(self, job_id: int, worker: str, result: Any = None) -> Job:
        """Mark a running job done. Wrong or stale worker raises ``LeaseLost``.

        This is the second half of at-least-once delivery: a worker that was
        reaped must not be able to overwrite the state of the worker that
        took over.
        """
        now = self.clock()
        with self._tx() as cursor:
            cursor.execute(
                "UPDATE jobs SET state = ?, result = ?, worker = NULL, lease_expires_at = NULL, "
                "updated_at = ?, last_error = NULL WHERE id = ? AND state = ? AND worker = ?",
                (DONE, encode_payload(result) if result is not None else None, now, job_id,
                 RUNNING, worker),
            )
            if cursor.rowcount != 1:
                self._explain_failure(cursor, job_id, worker)
        return self.get(job_id)  # type: ignore[return-value]

    def fail(
        self,
        job_id: int,
        worker: str,
        error: str,
        *,
        retry_in: Optional[float] = None,
    ) -> Job:
        """Record a failure. Schedules a retry, or dead-letters on the last attempt."""
        now = self.clock()
        with self._tx() as cursor:
            row = cursor.execute(
                "SELECT attempts, max_attempts FROM jobs "
                "WHERE id = ? AND state = ? AND worker = ?",
                (job_id, RUNNING, worker),
            ).fetchone()
            if row is None:
                self._explain_failure(cursor, job_id, worker)

            attempts = int(row["attempts"])
            exhausted = attempts >= int(row["max_attempts"])
            if exhausted:
                cursor.execute(
                    "UPDATE jobs SET state = ?, last_error = ?, worker = NULL, "
                    "lease_expires_at = NULL, updated_at = ? WHERE id = ?",
                    (DEAD, error, now, job_id),
                )
            else:
                delay = retry_in if retry_in is not None else self._delay(attempts)
                if delay < 0:
                    raise ValueError("retry_in must be >= 0")
                cursor.execute(
                    "UPDATE jobs SET state = ?, last_error = ?, worker = NULL, "
                    "lease_expires_at = NULL, run_at = ?, updated_at = ? WHERE id = ?",
                    (PENDING, error, now + float(delay), now, job_id),
                )
        return self.get(job_id)  # type: ignore[return-value]

    def kill(self, job_id: int, worker: str, error: str) -> Job:
        """Dead-letter a running job now: the error is not worth retrying.

        ``fail`` counts attempts and backs off; a permanent error (a malformed
        payload, an unknown task) would burn every retry and delay the dead
        letter by minutes. This is the "stop, do not retry" path.
        """
        now = self.clock()
        with self._tx() as cursor:
            cursor.execute(
                "UPDATE jobs SET state = ?, last_error = ?, worker = NULL, "
                "lease_expires_at = NULL, updated_at = ? WHERE id = ? AND state = ? AND worker = ?",
                (DEAD, error, now, job_id, RUNNING, worker),
            )
            if cursor.rowcount != 1:
                self._explain_failure(cursor, job_id, worker)
        return self.get(job_id)  # type: ignore[return-value]

    def _delay(self, attempts: int) -> float:
        return backoff_mod.compute_delay(
            attempts,
            base=self.backoff_base,
            cap=self.backoff_cap,
            jitter=self.jitter,
            rng=self.rng,
        )

    def _explain_failure(self, cursor: sqlite3.Cursor, job_id: int, worker: str) -> "NoReturn":
        row = cursor.execute("SELECT state, worker FROM jobs WHERE id = ?", (job_id,)).fetchone()
        if row is None:
            raise InvalidState(f"job {job_id} does not exist")
        if row["state"] != RUNNING:
            raise LeaseLost(f"job {job_id} is {row['state']}, not running under {worker!r}")
        raise LeaseLost(f"job {job_id} is leased to {row['worker']!r}, not {worker!r}")

    # -- recovery ----------------------------------------------------------

    def reap(self, *, now: Optional[float] = None) -> ReapResult:
        """Return jobs whose lease expired: retry them, or dead-letter them.

        A crashed worker never calls ``fail``. Without this, its job stays
        running forever and the queue's throughput quietly drops.
        """
        moment = self.clock() if now is None else float(now)
        recovered: List[int] = []
        dead: List[int] = []

        with self._tx() as cursor:
            rows = cursor.execute(
                "SELECT id, attempts, max_attempts FROM jobs "
                "WHERE state = ? AND lease_expires_at IS NOT NULL AND lease_expires_at <= ? "
                "ORDER BY id",
                (RUNNING, moment),
            ).fetchall()
            for row in rows:
                if int(row["attempts"]) >= int(row["max_attempts"]):
                    cursor.execute(
                        "UPDATE jobs SET state = ?, last_error = ?, worker = NULL, "
                        "lease_expires_at = NULL, updated_at = ? WHERE id = ?",
                        (DEAD, "lease expired", moment, row["id"]),
                    )
                    dead.append(int(row["id"]))
                else:
                    cursor.execute(
                        "UPDATE jobs SET state = ?, worker = NULL, lease_expires_at = NULL, "
                        "run_at = ?, updated_at = ? WHERE id = ?",
                        (PENDING, moment, moment, row["id"]),
                    )
                    recovered.append(int(row["id"]))
        return ReapResult(tuple(recovered), tuple(dead))

    # -- operator actions --------------------------------------------------

    def cancel(self, job_id: int) -> Job:
        """Withdraw a job that has not started. Running jobs must fail first."""
        now = self.clock()
        with self._tx() as cursor:
            cursor.execute(
                "UPDATE jobs SET state = ?, updated_at = ? WHERE id = ? AND state = ?",
                (CANCELLED, now, job_id, PENDING),
            )
            if cursor.rowcount != 1:
                row = cursor.execute("SELECT state FROM jobs WHERE id = ?", (job_id,)).fetchone()
                if row is None:
                    raise InvalidState(f"job {job_id} does not exist")
                raise InvalidState(f"job {job_id} is {row['state']}; only pending jobs cancel")
        return self.get(job_id)  # type: ignore[return-value]

    def requeue(self, job_id: int) -> Job:
        """Retry a dead job from scratch. Operator intent, so attempts reset."""
        now = self.clock()
        with self._tx() as cursor:
            cursor.execute(
                "UPDATE jobs SET state = ?, attempts = 0, run_at = ?, worker = NULL, "
                "lease_expires_at = NULL, last_error = NULL, updated_at = ? "
                "WHERE id = ? AND state = ?",
                (PENDING, now, now, job_id, DEAD),
            )
            if cursor.rowcount != 1:
                row = cursor.execute("SELECT state FROM jobs WHERE id = ?", (job_id,)).fetchone()
                if row is None:
                    raise InvalidState(f"job {job_id} does not exist")
                raise InvalidState(f"job {job_id} is {row['state']}; only dead jobs requeue")
        return self.get(job_id)  # type: ignore[return-value]

    def purge(
        self,
        *,
        older_than: float = 0.0,
        states: Sequence[str] = (DONE, CANCELLED, DEAD),
    ) -> int:
        """Delete terminal jobs older than ``older_than`` seconds. Returns a count."""
        unknown = [state for state in states if state not in JOB_STATES]
        if unknown:
            raise ValueError(f"unknown state(s): {unknown}")
        cutoff = self.clock() - float(older_than)
        placeholders = ", ".join("?" for _ in states)
        with self._tx() as cursor:
            cursor.execute(
                f"DELETE FROM jobs WHERE state IN ({placeholders}) AND updated_at <= ?",
                (*states, cutoff),
            )
            return int(cursor.rowcount)

    # -- reads -------------------------------------------------------------

    def get(self, job_id: int) -> Optional[Job]:
        with self._lock:
            row = self._conn.execute(
                f"SELECT {FIELDS} FROM jobs WHERE id = ?", (job_id,)
            ).fetchone()
        return Job.from_row(row) if row is not None else None

    def list(
        self,
        *,
        state: Optional[str] = None,
        queue: Optional[str] = None,
        limit: int = 50,
    ) -> List[Job]:
        if state is not None and state not in JOB_STATES:
            raise ValueError(f"unknown state: {state}")
        if limit < 1:
            raise ValueError("limit must be >= 1")
        clauses = []
        params: List[Any] = []
        if state is not None:
            clauses.append("state = ?")
            params.append(state)
        if queue is not None:
            clauses.append("queue = ?")
            params.append(queue)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._lock:
            rows = self._conn.execute(
                f"SELECT {FIELDS} FROM jobs{where} "
                "ORDER BY priority DESC, run_at ASC, id ASC LIMIT ?",
                (*params, limit),
            ).fetchall()
        return [Job.from_row(row) for row in rows]

    def stats(self, *, queue: Optional[str] = None) -> Stats:
        now = self.clock()
        clause = " WHERE queue = ?" if queue is not None else ""
        params: Tuple[Any, ...] = (queue,) if queue is not None else ()
        with self._lock:
            rows = self._conn.execute(
                f"SELECT state, COUNT(*) AS n FROM jobs{clause} GROUP BY state", params
            ).fetchall()
            counts = {state: 0 for state in JOB_STATES}
            for row in rows:
                counts[str(row["state"])] = int(row["n"])

            ready = self._conn.execute(
                f"SELECT COUNT(*) AS n FROM jobs{clause} "
                f"{'AND' if clause else 'WHERE'} state = ? AND run_at <= ?",
                (*params, PENDING, now),
            ).fetchone()["n"]
            delayed = self._conn.execute(
                f"SELECT COUNT(*) AS n FROM jobs{clause} "
                f"{'AND' if clause else 'WHERE'} state = ? AND run_at > ?",
                (*params, PENDING, now),
            ).fetchone()["n"]
            expired = self._conn.execute(
                f"SELECT COUNT(*) AS n FROM jobs{clause} "
                f"{'AND' if clause else 'WHERE'} state = ? AND lease_expires_at IS NOT NULL "
                "AND lease_expires_at <= ?",
                (*params, RUNNING, now),
            ).fetchone()["n"]
            oldest = self._conn.execute(
                f"SELECT MIN(run_at) AS oldest FROM jobs{clause} "
                f"{'AND' if clause else 'WHERE'} state = ?",
                (*params, PENDING),
            ).fetchone()["oldest"]

        return Stats(
            counts=counts,
            ready=int(ready),
            delayed=int(delayed),
            expired_leases=int(expired),
            oldest_pending_age=None if oldest is None else max(0.0, now - float(oldest)),
        )
