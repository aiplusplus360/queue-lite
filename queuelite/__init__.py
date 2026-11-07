"""queue-lite: a durable job queue that is one SQLite file and zero services.

    from queuelite import Queue, Worker

    queue = Queue("jobs.db")
    queue.add({"task": "resize", "photo": 42})

    worker = Worker(queue, {"resize": lambda payload: {"pixels": payload["photo"] * 2}})
    worker.run_once()

Jobs survive restarts because they are rows, not memory. A job is leased, not
locked: if the worker dies, the lease expires and the job comes back. Delivery
is at-least-once, so handlers should be idempotent -- and ``unique_key`` lets the
producer be honest about that.
"""

from __future__ import annotations

import logging

from .backoff import compute_delay
from .engine import AddResult, InvalidState, LeaseLost, Queue, ReapResult, Stats
from .job import CANCELLED, DEAD, DONE, JOB_STATES, PENDING, RUNNING, Job, encode_payload
from .worker import PermanentFailure, RunStats, UnregisteredTask, Worker

__all__ = [
    "AddResult",
    "CANCELLED",
    "DEAD",
    "DONE",
    "InvalidState",
    "JOB_STATES",
    "Job",
    "LeaseLost",
    "PENDING",
    "PermanentFailure",
    "Queue",
    "RUNNING",
    "ReapResult",
    "RunStats",
    "Stats",
    "UnregisteredTask",
    "Worker",
    "compute_delay",
    "encode_payload",
]

__version__ = "1.0.0"

# A library never configures logging for its host, but a NullHandler stops the
# root logger's last-resort handler from printing warnings nobody asked for.
logging.getLogger(__name__).addHandler(logging.NullHandler())
