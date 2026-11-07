"""The job record and its state machine.

Five states, because a queue with more than five is a workflow engine:

    pending    waiting, either because it is new or because a retry is due
    running    leased to a worker until ``lease_expires_at``
    done       finished, with a result
    dead       attempts exhausted; kept for inspection and requeue
    cancelled  withdrawn before it ran

There is no ``failed`` state on purpose. A job that failed but will be retried
is still pending -- it has a ``run_at`` in the future and a ``last_error``. A
separate "failed" state would mean two different things depending on whether a
scheduler had run yet, and every query would have to know which.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping, Optional

__all__ = ["JOB_STATES", "PENDING", "RUNNING", "DONE", "DEAD", "CANCELLED", "Job", "encode_payload"]

PENDING = "pending"
RUNNING = "running"
DONE = "done"
DEAD = "dead"
CANCELLED = "cancelled"

JOB_STATES = (PENDING, RUNNING, DONE, DEAD, CANCELLED)


def _encode(value: Any, field_name: str) -> Optional[str]:
    if value is None:
        return None
    try:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} is not JSON-serialisable: {exc}") from exc


def _decode(text: Optional[str]) -> Any:
    if not text:
        return None
    return json.loads(text)


def encode_payload(payload: Any) -> str:
    """JSON text for storage. Kept here so the engine never invents a format."""
    return _encode(payload if payload is not None else {}, "payload") or "{}"


@dataclass(frozen=True)
class Job:
    """One unit of work. Immutable; the engine returns a new object after writes."""

    id: int
    queue: str
    payload: Mapping[str, Any]
    state: str
    priority: int = 0
    attempts: int = 0
    max_attempts: int = 3
    run_at: float = 0.0
    lease_expires_at: Optional[float] = None
    worker: Optional[str] = None
    last_error: Optional[str] = None
    unique_key: Optional[str] = None
    result: Any = None
    created_at: float = 0.0
    updated_at: float = 0.0

    @property
    def terminal(self) -> bool:
        return self.state in (DONE, DEAD, CANCELLED)

    @property
    def retries_left(self) -> int:
        return max(0, self.max_attempts - self.attempts)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "queue": self.queue,
            "payload": dict(self.payload),
            "state": self.state,
            "priority": self.priority,
            "attempts": self.attempts,
            "max_attempts": self.max_attempts,
            "retries_left": self.retries_left,
            "run_at": self.run_at,
            "lease_expires_at": self.lease_expires_at,
            "worker": self.worker,
            "last_error": self.last_error,
            "unique_key": self.unique_key,
            "result": self.result,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "Job":
        return cls(
            id=int(row["id"]),
            queue=str(row["queue"]),
            payload=_decode(row["payload"]) or {},
            state=str(row["state"]),
            priority=int(row["priority"]),
            attempts=int(row["attempts"]),
            max_attempts=int(row["max_attempts"]),
            run_at=float(row["run_at"]),
            lease_expires_at=(
                None if row["lease_expires_at"] is None else float(row["lease_expires_at"])
            ),
            worker=row["worker"],
            last_error=row["last_error"],
            unique_key=row["unique_key"],
            result=_decode(row["result"]),
            created_at=float(row["created_at"]),
            updated_at=float(row["updated_at"]),
        )
