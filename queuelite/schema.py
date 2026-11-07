"""Schema, and the three SQL details that make a queue safe.

1. **A partial unique index on `unique_key`** gives deduplication for free:
   only *active* jobs (pending or running) are constrained, so the same key can
   be enqueued again once the previous job finished.
2. **`BEGIN IMMEDIATE` around the claim** is what stops two workers taking the
   same row. A deferred transaction would let both sessions read the same
   pending row before either writes.
3. **`lease_expires_at` on the row, not in the worker's memory.** A worker that
   crashes holds no lock; it holds a timestamp other processes can see.

The DDL is SQLite-flavoured. For Postgres only the `id` column changes
(`GENERATED ALWAYS AS IDENTITY`); every query in ``engine.py`` is plain SQL
with `?` placeholders, which psycopg accepts as `%s` after a swap.
"""

from __future__ import annotations

__all__ = ["SCHEMA", "INDEXES", "POSTGRES_NOTE"]

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    queue            TEXT    NOT NULL,
    payload          TEXT    NOT NULL,
    state            TEXT    NOT NULL,
    priority         INTEGER NOT NULL DEFAULT 0,
    attempts         INTEGER NOT NULL DEFAULT 0,
    max_attempts     INTEGER NOT NULL DEFAULT 3,
    run_at           REAL    NOT NULL,
    lease_expires_at REAL,
    worker           TEXT,
    last_error       TEXT,
    unique_key       TEXT,
    result           TEXT,
    created_at       REAL    NOT NULL,
    updated_at       REAL    NOT NULL,
    CHECK (state IN ('pending', 'running', 'done', 'dead', 'cancelled'))
);
"""

INDEXES = """
CREATE INDEX IF NOT EXISTS jobs_claim
    ON jobs (queue, state, run_at, priority, id);

CREATE INDEX IF NOT EXISTS jobs_lease
    ON jobs (state, lease_expires_at);

CREATE UNIQUE INDEX IF NOT EXISTS jobs_unique_active
    ON jobs (unique_key)
    WHERE unique_key IS NOT NULL AND state IN ('pending', 'running');
"""

POSTGRES_NOTE = """
Postgres equivalent of the id column, and nothing else needs changing:

    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY

`REAL` should become `double precision` (or `timestamptz` if you prefer real
timestamps to epoch seconds). The partial unique index and the claim query work
unchanged.
"""
