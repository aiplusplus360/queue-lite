# queue-lite

**A durable job queue in one SQLite file.** No broker, no Redis, no daemon. Leases, exponential backoff with jitter, dead letters, deduplication, and a CLI — in well under two thousand lines of standard-library Python.

[![CI](https://github.com/aiplusplus360/queue-lite/actions/workflows/ci.yml/badge.svg)](https://github.com/aiplusplus360/queue-lite/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.9%20%7C%203.11%20%7C%203.13-blue)](pyproject.toml)
[![Dependencies](https://img.shields.io/badge/dependencies-none-brightgreen)](pyproject.toml)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

```python
from queuelite import Queue, Worker

queue = Queue("jobs.db")
queue.add({"task": "resize", "photo": "cover.jpg"}, priority=10)

worker = Worker(queue, {"resize": lambda payload: {"ok": payload["photo"]}})
worker.run_forever()
```

That is a complete producer and consumer. The jobs are rows; they survive a reboot.

---

## Why this exists

Most projects need a background queue long before they need Kafka. The usual choices are a managed service (cost, latency, another account), Redis + RQ/Celery (a second service to run and monitor), or a `try/except` around a thread (works until the process restarts).

If your throughput fits on one disk — cron jobs, email sends, thumbnail generation, webhook fan-out, nightly syncs — SQLite already gives you ACID writes, concurrent readers, and a single file you can back up with `cp`. What it does not give you is a queue protocol. queue-lite is that protocol:

| | queue-lite | Celery + Redis | RQ | Postgres SKIP LOCKED |
|---|---|---|---|---|
| Services to run | **0** | 2+ | 2 | 1 |
| Durable across restarts | **yes** | with config | yes | yes |
| Leases that recover a crashed worker | **yes** | partial | yes | manual |
| Deduplication | **built in** | plugin | no | manual |
| Dead letters + requeue | **built in** | plugin | partial | manual |
| Observability | **one SQL table** | Flower | dashboard | psql |
| Dependencies | **none** | several | several | driver |

It is not a replacement for a cluster-wide broker at 100k jobs/second. It is a replacement for the thirty lines of `threading.Thread` you were about to write.

---

## Install

```bash
pip install queue-lite
```

Python 3.9+. No runtime dependencies. SQLite ships with CPython.

From a checkout:

```bash
pip install -e ".[dev]"
python examples/demo.py
python -m unittest discover -s tests -v
```

---

## The four guarantees, and the code behind each

### 1. A job is a row, so a restart is not a data loss event

```python
result = queue.add({"task": "send", "to": "a@example.com"}, delay=300)
print(result.id, result.duplicate)
```

`add` commits before it returns. The payload is JSON-encoded with sorted keys, so identical payloads are byte-identical in the database and easy to inspect by eye.

### 2. A claim is a lease, not a lock

```python
job = queue.take(worker="worker-1")[0]
# job.state == "running", job.lease_expires_at = now + 300
queue.complete(job.id, job.worker, result={"delivered": True})
```

`take` runs `BEGIN IMMEDIATE` and updates the row inside the transaction, so two workers racing for one job cannot both win. The claim is a timestamp, not a held lock: a worker that is `kill -9`'d holds nothing, and after the lease expires the job returns to the pool.

A worker that lost its lease cannot overwrite the row — `complete` and `fail` match on `state = 'running' AND worker = ?`, so the stale worker gets a `LeaseLost` exception instead of silently clobbering the worker that took over.

### 3. Retries back off, with jitter

```python
from queuelite import compute_delay

compute_delay(1)   # 2.0 s, ±20%
compute_delay(5)   # 32.0 s
compute_delay(20)  # capped at 300 s
```

Exponential backoff without jitter synchronises every job that failed during an outage onto the same retry instant, which is the moment the upstream is least able to handle them. The jitter here is multiplicative and bounded, so `1.0×`—`1.2×` of a known delay, not a random number you cannot reason about.

Failures that retrying cannot fix are dead-lettered immediately instead of burning every attempt:

```python
from queuelite import PermanentFailure

def handler(payload):
    if "order_id" not in payload:
        raise PermanentFailure("payload v1 is no longer supported")
    ...
```

The worker routes `PermanentFailure` (and unknown task names) through `queue.kill`, which sets `state = 'dead'` on the first attempt. Inspect and retry them with two commands:

```bash
queuelite list --state dead
queuelite requeue --id 42
```

### 4. Duplicate work is prevented at the index, not in application code

```python
first  = queue.add({"user": 7}, unique_key="welcome-email:7")
second = queue.add({"user": 7}, unique_key="welcome-email:7")
assert second.duplicate and second.id == first.id
```

The guarantee is a partial unique index:

```sql
CREATE UNIQUE INDEX jobs_unique_active
    ON jobs (unique_key)
    WHERE unique_key IS NOT NULL AND state IN ('pending', 'running');
```

Two producers can race and the database picks one. The key is released the moment the job reaches `done`, `dead`, or `cancelled`, so the same key can be enqueued again for the next unit of work. This is what makes at-least-once delivery safe in practice: the *consumer* may run twice, but the *producer* does not create the duplicate in the first place.

---

## Command line

Everything the library does is available from the shell, which is what makes it operable at 3 a.m.:

```bash
# queue a job
queuelite --db jobs.db add --payload '{"task":"resize","photo":"a.jpg"}' --priority 10

# what is going on?
queuelite --db jobs.db stats
queuelite --db jobs.db list --state dead

# run the queue through a handler, then exit
queuelite --db jobs.db work --handler myapp.jobs:handle

# keep polling forever
queuelite --db jobs.db work --handler myapp.jobs:handle --follow

# recover jobs from a process that died mid-flight
queuelite --db jobs.db reap

# operator actions
queuelite --db jobs.db cancel --id 7
queuelite --db jobs.db requeue --id 9
queuelite --db jobs.db purge --older-than 86400
```

`--db`, `--lease`, `--max-attempts`, and `--jitter` are accepted before or after the subcommand. Add `--json` to any read command for machine-readable output.

Exit codes are part of the contract:

| Code | Meaning |
|---|---|
| `0` | done |
| `1` | nothing to do (`take` found no ready job) |
| `2` | bad input (invalid JSON, missing handler module) |
| `4` | the lease was lost to another worker |
| `5` | the job is in a state that forbids this transition |

Example session:

```console
$ queuelite add --payload '{"task":"resize"}' --json
{"id": 1, "duplicate": false}
$ queuelite stats
pending 1  running 0  done 0  dead 0  cancelled 0
ready 1  delayed 0  expired leases 0
```

---

## API

### `Queue(path=":memory:", *, clock=time.time, lease_seconds=300, default_max_attempts=3, backoff_base=2.0, backoff_cap=300.0, jitter=0.2)`

| Method | Behaviour |
|---|---|
| `add(payload, *, queue="default", priority=0, delay=0, max_attempts=None, unique_key=None)` | Enqueue. Returns `AddResult(id, duplicate)`. |
| `take(limit=1, *, queue="default", worker=None)` | Claim up to `limit` ready jobs. Priority desc, then `run_at`, then id. |
| `heartbeat(job_id, worker, *, extend=None)` | Extend a lease from a long-running handler. Raises `LeaseLost`. |
| `complete(job_id, worker, result=None)` | Mark done and store a JSON result. |
| `fail(job_id, worker, error, *, retry_in=None)` | Retry with backoff, or dead-letter on the last attempt. |
| `kill(job_id, worker, error)` | Dead-letter now, whatever the attempt count. |
| `reap(*, now=None)` | Return expired leases to the pool. Returns `ReapResult(recovered, dead)`. |
| `cancel(job_id)` / `requeue(job_id)` | Withdraw a pending job / reset a dead one. |
| `purge(*, older_than=0, states=("done","cancelled","dead"))` | Delete terminal jobs. Returns the count. |
| `get(id)` / `list(state=None, queue=None, limit=50)` / `stats(queue=None)` | Reads. |
| `fork(**overrides)` | A second connection to the same file, for another thread. |

`clock` is injectable, which is how the test suite checks lease expiry and backoff schedules in microseconds instead of by sleeping.

### `Worker(queue, handlers=None, *, queues=("default",), concurrency=1, poll_interval=1.0, worker_id=None)`

```python
worker = Worker(queue, {"resize": resize, "email": send_email})
worker.register("export", export)      # chainable
worker.run_once(limit=5)               # one batch
worker.run_until_idle()                # drain what is ready, then return
worker.run_forever()                   # poll until worker.stop()
```

- With one handler, every job goes to it. With several, the payload's `"task"` selects the handler; an unknown task is a `PermanentFailure`.
- A handler receives the payload dict and returns any JSON-serialisable result.
- `concurrency > 1` starts threads, each with its own SQLite connection. Requires a file-backed database (`:memory:` databases are per-connection).
- While a handler runs, a background thread extends the lease every `lease_seconds / 3`, so a legitimately slow job is not reaped out from under a healthy worker.
- `worker.install_signal_handlers()` turns `SIGINT`/`SIGTERM` into a graceful stop: the current job finishes, then the loop exits.

### Crash recovery

A worker never gets a chance to call `fail` when the process is killed. Run the reaper from cron or a sidecar:

```bash
* * * * * queuelite --db /srv/app/jobs.db reap
```

Or call `queue.reap()` at the top of your worker loop — `Worker.run_forever()` already does.

---

## How the state machine works

```
              add
               │
               ▼
          ┌─────────┐  take   ┌─────────┐  complete  ┌──────┐
          │ pending │────────▶│ running │───────────▶│ done │
          └─────────┘         └─────────┘            └──────┘
             ▲   │  ▲              │
   requeue   │   │  │  fail/backoff│  lease expiry (reap)
             │   │  └──────────────┘
          ┌──┴───▼──┐              │
          │  dead   │◀─────────────┘  attempts exhausted / kill
          └─────────┘
```

There is deliberately no `failed` state. A job that failed but will be retried is still `pending`, with a future `run_at` and a `last_error`. A separate `failed` state would mean two different things depending on whether a scheduler had run yet, and every query would have to know which.

---

## SQLite today, Postgres tomorrow

All queries live in `queuelite/engine.py` and use `?` placeholders, which psycopg accepts as `%s` after a swap. The schema is in `queuelite/schema.py`; the only change is the identity column:

```sql
id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY
```

`REAL` becomes `double precision`, and the claim query becomes `SELECT ... FOR UPDATE SKIP LOCKED`. The partial unique index and every state transition work unchanged.

---

## What it is not

- **Not a scheduler.** No cron expressions. Enqueue delayed jobs (`delay=`) or put `queue-lite add` in cron.
- **Not exactly-once.** At-least-once is the honest promise for any queue that survives a crash. Use `unique_key` on the producer side and idempotent handlers on the consumer side.
- **Not distributed.** One writer at a time, which SQLite enforces. That is a feature at this scale: the throughput ceiling is roughly tens of thousands of jobs per second on a laptop, and the failure modes are a file, not a network partition.

---

## Testing

```bash
python -m unittest discover -s tests -v      # 73 tests
python -m doctest -v queuelite/backoff.py    # examples in the docstrings
python examples/demo.py                      # end-to-end tour
```

The suite covers concurrent claims from four threads on one file, a threaded worker pool starting and stopping cleanly, lease recovery, stale-worker rejection, backoff schedules, deduplication, dead letters, the CLI exit-code contract, and `fork` sharing.

CI runs Python 3.9, 3.11, and 3.13, then builds a wheel and exercises the installed console script.

---

## License

MIT © 2026 aiplusplus360
