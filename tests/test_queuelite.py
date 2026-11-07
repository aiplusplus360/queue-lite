"""Tests for queue-lite. Standard library only: ``python -m unittest discover -s tests``."""

from __future__ import annotations

import io
import json
import os
import sys
import tempfile
import threading
import time
import unittest
from contextlib import redirect_stderr, redirect_stdout

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from queuelite import (  # noqa: E402
    CANCELLED,
    DEAD,
    DONE,
    PENDING,
    RUNNING,
    InvalidState,
    Job,
    LeaseLost,
    PermanentFailure,
    Queue,
    UnregisteredTask,
    Worker,
    compute_delay,
)
from queuelite import cli  # noqa: E402
from queuelite.job import encode_payload  # noqa: E402


class Clock:
    """A clock you can move, so no test sleeps for a lease to expire."""

    def __init__(self, start: float = 1000.0) -> None:
        self.now = float(start)

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += float(seconds)


def make_queue(**overrides):
    clock = overrides.pop("clock", None) or Clock()
    options = dict(clock=clock, lease_seconds=30.0, jitter=0.0)
    options.update(overrides)
    return Queue(":memory:", **options), clock


class QueueBasicsTest(unittest.TestCase):
    def test_add_and_take_sets_lease_and_attempt(self):
        queue, _clock = make_queue()
        added = queue.add({"task": "a"})
        self.assertFalse(added.duplicate)

        job = queue.take()[0]
        self.assertEqual(job.id, added.id)
        self.assertEqual(job.state, RUNNING)
        self.assertEqual(job.attempts, 1)
        self.assertEqual(job.payload, {"task": "a"})
        self.assertEqual(job.lease_expires_at, 1030.0)

    def test_complete_stores_result_and_clears_lease(self):
        queue, _clock = make_queue()
        job = queue.add({"n": 1})
        taken = queue.take()[0]
        done = queue.complete(taken.id, taken.worker, {"ok": True})

        self.assertEqual(done.state, DONE)
        self.assertEqual(done.result, {"ok": True})
        self.assertIsNone(done.worker)
        self.assertIsNone(done.lease_expires_at)
        self.assertIsNone(done.last_error)
        self.assertTrue(done.terminal)
        self.assertEqual(queue.take(), [])
        self.assertEqual(done.id, job.id)

    def test_payload_defaults_to_an_empty_dict(self):
        queue, _clock = make_queue()
        queue.add()
        self.assertEqual(queue.list()[0].payload, {})

    def test_payload_round_trips_unicode_and_nesting(self):
        queue, _clock = make_queue()
        payload = {"name": "café ☕", "nested": {"items": [1, 2.5, None, True]}}
        job = queue.add(payload)
        self.assertEqual(queue.get(job.id).payload, payload)

    def test_non_json_payload_is_rejected_before_it_is_stored(self):
        queue, _clock = make_queue()
        with self.assertRaises(ValueError):
            queue.add({"when": {1, 2}})
        self.assertEqual(queue.stats().ready, 0)

    def test_dedup_returns_the_active_job(self):
        queue, _clock = make_queue()
        first = queue.add({"n": 1}, unique_key="user-7")
        second = queue.add({"n": 2}, unique_key="user-7")

        self.assertFalse(first.duplicate)
        self.assertTrue(second.duplicate)
        self.assertEqual(second.id, first.id)
        self.assertEqual(queue.stats().counts[PENDING], 1)

    def test_dedup_survives_a_claim_but_ends_at_a_terminal_state(self):
        queue, _clock = make_queue()
        first = queue.add(unique_key="k")
        taken = queue.take()[0]
        self.assertTrue(queue.add(unique_key="k").duplicate)

        queue.complete(taken.id, taken.worker)
        again = queue.add(unique_key="k")
        self.assertFalse(again.duplicate)
        self.assertNotEqual(again.id, first.id)

    def test_cancelled_jobs_free_their_unique_key(self):
        queue, _clock = make_queue()
        first = queue.add(unique_key="k")
        queue.cancel(first.id)
        self.assertFalse(queue.add(unique_key="k").duplicate)

    def test_dedup_is_scoped_to_the_key_not_the_queue(self):
        queue, _clock = make_queue()
        queue.add(queue="a", unique_key="same")
        self.assertTrue(queue.add(queue="b", unique_key="same").duplicate)

    def test_add_validates_input(self):
        queue, _clock = make_queue()
        with self.assertRaises(ValueError):
            queue.add(queue="")
        with self.assertRaises(ValueError):
            queue.add(delay=-1)
        with self.assertRaises(ValueError):
            queue.add(max_attempts=0)
        with self.assertRaises(ValueError):
            queue.take(0)
        with self.assertRaises(ValueError):
            Queue(":memory:", lease_seconds=0)
        with self.assertRaises(ValueError):
            Queue(":memory:", default_max_attempts=0)


class OrderingTest(unittest.TestCase):
    def test_priority_beats_arrival_order(self):
        queue, _clock = make_queue()
        low = queue.add(priority=0)
        high = queue.add(priority=10)
        mid = queue.add(priority=5)

        ids = [job.id for job in queue.take(3)]
        self.assertEqual(ids, [high.id, mid.id, low.id])

    def test_same_priority_is_first_in_first_out(self):
        queue, _clock = make_queue()
        ids = [queue.add().id for _ in range(3)]
        self.assertEqual([job.id for job in queue.take(3)], ids)

    def test_delayed_jobs_are_not_ready_until_their_run_at(self):
        queue, clock = make_queue()
        job = queue.add({"later": True}, delay=60)
        self.assertEqual(queue.take(), [])
        self.assertEqual(queue.stats().delayed, 1)
        self.assertEqual(queue.stats().ready, 0)

        clock.advance(60)
        self.assertEqual([taken.id for taken in queue.take()], [job.id])
        self.assertEqual(queue.stats().delayed, 0)

    def test_queues_are_isolated(self):
        queue, _clock = make_queue()
        queue.add(queue="media")
        self.assertEqual(queue.take(), [])
        self.assertEqual(queue.take(queue="media")[0].queue, "media")

    def test_take_limit_is_honoured(self):
        queue, _clock = make_queue()
        for _ in range(5):
            queue.add()
        self.assertEqual(len(queue.take(2)), 2)
        self.assertEqual(len(queue.take(10)), 3)


class LeaseTest(unittest.TestCase):
    def test_a_lost_lease_is_recovered_and_attempts_are_kept(self):
        queue, clock = make_queue(lease_seconds=10.0)
        job = queue.add()
        taken = queue.take()[0]

        clock.advance(11)
        result = queue.reap()
        self.assertEqual(result.recovered, (taken.id,))
        self.assertEqual(result.dead, ())

        again = queue.take()[0]
        self.assertEqual(again.id, job.id)
        self.assertEqual(again.attempts, 2)
        self.assertIsNone(again.last_error)

    def test_reap_dead_letters_a_job_on_its_last_attempt(self):
        queue, clock = make_queue(lease_seconds=5.0)
        job = queue.add(max_attempts=1)
        queue.take()
        clock.advance(6)

        result = queue.reap()
        self.assertEqual(result.dead, (job.id,))
        state = queue.get(job.id)
        self.assertEqual(state.state, DEAD)
        self.assertIn("lease expired", state.last_error)

    def test_reap_is_a_no_op_before_expiry(self):
        queue, clock = make_queue(lease_seconds=100.0)
        queue.take()
        clock.advance(99)
        self.assertEqual(len(queue.reap()), 0)

    def test_a_reaped_job_cannot_be_completed_by_the_old_worker(self):
        queue, clock = make_queue(lease_seconds=10.0)
        queue.add()
        stale = queue.take()[0]

        clock.advance(11)
        queue.reap()
        new_worker_job = queue.take()[0]
        self.assertNotEqual(new_worker_job.worker, stale.worker)

        with self.assertRaises(LeaseLost):
            queue.complete(stale.id, stale.worker or "")
        self.assertEqual(queue.complete(new_worker_job.id, new_worker_job.worker).state, DONE)

    def test_wrong_worker_and_missing_job_report_distinct_errors(self):
        queue, _clock = make_queue()
        queue.add()
        job = queue.take()[0]

        with self.assertRaises(LeaseLost):
            queue.complete(job.id, "someone-else")
        with self.assertRaises(InvalidState):
            queue.complete(9999, "someone-else")

    def test_heartbeat_extends_the_lease(self):
        queue, clock = make_queue(lease_seconds=10.0)
        queue.add()
        job = queue.take()[0]

        clock.advance(5)
        renewed = queue.heartbeat(job.id, job.worker, extend=60)
        self.assertEqual(renewed.lease_expires_at, 1065.0)

        clock.advance(50)
        self.assertEqual(len(queue.reap()), 0)
        self.assertEqual(queue.get(job.id).state, RUNNING)

    def test_heartbeat_rejects_a_stale_worker(self):
        queue, _clock = make_queue()
        queue.add()
        job = queue.take()[0]
        with self.assertRaises(LeaseLost):
            queue.heartbeat(job.id, "impostor")
        with self.assertRaises(ValueError):
            queue.heartbeat(job.id, job.worker or "", extend=0)


class FailureTest(unittest.TestCase):
    def test_first_failure_schedules_an_exponential_retry(self):
        queue, clock = make_queue(backoff_base=2.0, backoff_cap=300.0)
        queue.add(max_attempts=3)
        job = queue.take()[0]

        clock.advance(1)
        failed = queue.fail(job.id, job.worker, "boom")
        self.assertEqual(failed.state, PENDING)
        self.assertEqual(failed.last_error, "boom")
        self.assertEqual(failed.run_at, 1003.0)  # now 1001 + 2**1
        self.assertEqual(queue.stats().delayed, 1)

    def test_retry_delays_grow_then_cap(self):
        queue, clock = make_queue(backoff_base=2.0, backoff_cap=10.0)
        queue.add(max_attempts=5)
        delays = []
        for _ in range(4):
            job = queue.take()[0]
            queue.fail(job.id, job.worker, "nope")
            delays.append(round(queue.get(job.id).run_at - clock.now, 6))
            clock.advance(1000)  # clear the retry delay before the next attempt
        self.assertEqual(delays, [2.0, 4.0, 8.0, 10.0])

    def test_last_attempt_dead_letters_with_the_error(self):
        queue, clock = make_queue()
        job = queue.add(max_attempts=2)
        for _ in range(2):
            taken = queue.take()[0]
            last = queue.fail(taken.id, taken.worker, "still broken")
            clock.advance(60)
        self.assertEqual(last.state, DEAD)
        self.assertEqual(last.attempts, 2)
        self.assertEqual(last.last_error, "still broken")
        self.assertEqual(queue.stats().counts[DEAD], 1)
        self.assertEqual(queue.take(), [])
        self.assertEqual(job.id, last.id)

    def test_retry_in_overrides_the_backoff(self):
        queue, clock = make_queue()
        queue.add()
        job = queue.take()[0]
        clock.advance(1)
        failed = queue.fail(job.id, job.worker, "x", retry_in=7.5)
        self.assertEqual(failed.run_at, 1008.5)

    def test_fail_requires_the_current_lease(self):
        queue, _clock = make_queue()
        queue.add()
        job = queue.take()[0]
        with self.assertRaises(LeaseLost):
            queue.fail(job.id, "not-the-worker", "x")
        with self.assertRaises(ValueError):
            queue.fail(job.id, job.worker or "", "x", retry_in=-1)

    def test_kill_dead_letters_immediately_whatever_the_attempts(self):
        queue, _clock = make_queue()
        job = queue.add(max_attempts=10)
        taken = queue.take()[0]

        killed = queue.kill(taken.id, taken.worker, "permanent: bad payload")
        self.assertEqual(killed.state, DEAD)
        self.assertEqual(killed.attempts, 1)
        self.assertEqual(killed.last_error, "permanent: bad payload")
        self.assertEqual(queue.stats().counts[DEAD], 1)
        self.assertEqual(job.id, killed.id)

    def test_kill_rejects_an_unknown_lease(self):
        queue, _clock = make_queue()
        queue.add()
        job = queue.take()[0]
        with self.assertRaises(LeaseLost):
            queue.kill(job.id, "impostor", "x")


class OperatorTest(unittest.TestCase):
    def test_cancel_only_applies_to_pending_jobs(self):
        queue, _clock = make_queue()
        pending = queue.add()
        self.assertEqual(queue.cancel(pending.id).state, CANCELLED)

        other = queue.add()
        queue.take()
        with self.assertRaises(InvalidState):
            queue.cancel(other.id)
        with self.assertRaises(InvalidState):
            queue.cancel(4242)

    def test_requeue_resets_a_dead_job(self):
        queue, _clock = make_queue()
        queue.add(max_attempts=1)
        job = queue.take()[0]
        queue.fail(job.id, job.worker, "bad")

        back = queue.requeue(job.id)
        self.assertEqual(back.state, PENDING)
        self.assertEqual(back.attempts, 0)
        self.assertIsNone(back.last_error)
        self.assertEqual(queue.take()[0].attempts, 1)

    def test_requeue_rejects_live_jobs(self):
        queue, _clock = make_queue()
        job = queue.add()
        with self.assertRaises(InvalidState):
            queue.requeue(job.id)
        with self.assertRaises(InvalidState):
            queue.requeue(999)

    def test_purge_removes_only_terminal_jobs_older_than_the_cutoff(self):
        queue, clock = make_queue()
        done = queue.add()
        taken = queue.take()[0]
        queue.complete(taken.id, taken.worker)
        pending = queue.add()
        cancelled = queue.add()
        queue.cancel(cancelled.id)

        clock.advance(100)
        self.assertEqual(queue.purge(older_than=50), 2)
        self.assertIsNone(queue.get(done.id))
        self.assertIsNotNone(queue.get(pending.id))
        self.assertEqual(queue.stats().counts[PENDING], 1)

    def test_purge_rejects_unknown_states(self):
        queue, _clock = make_queue()
        with self.assertRaises(ValueError):
            queue.purge(states=("exploded",))


class ReadTest(unittest.TestCase):
    def test_get_returns_none_for_unknown_ids(self):
        queue, _clock = make_queue()
        self.assertIsNone(queue.get(12345))

    def test_list_filters_and_orders(self):
        queue, _clock = make_queue()
        low = queue.add(priority=0, queue="a")
        high = queue.add(priority=5, queue="a")
        queue.add(queue="b")

        self.assertEqual([j.id for j in queue.list(queue="a")], [high.id, low.id])
        self.assertEqual(len(queue.list(queue="b")), 1)
        self.assertEqual(len(queue.list(limit=2)), 2)
        with self.assertRaises(ValueError):
            queue.list(state="exploded")
        with self.assertRaises(ValueError):
            queue.list(limit=0)

    def test_stats_describe_the_whole_queue(self):
        queue, clock = make_queue(lease_seconds=10.0)
        finished = queue.add()
        taken = queue.take()[0]
        queue.complete(taken.id, taken.worker)

        queue.add()
        queue.add(delay=60)
        queue.add()
        running = queue.take()[0]

        clock.advance(11)
        stats = queue.stats()

        self.assertEqual(stats.counts[DONE], 1)
        self.assertEqual(stats.counts[RUNNING], 1)
        self.assertEqual(stats.counts[PENDING], 2)
        self.assertEqual(stats.ready, 1)
        self.assertEqual(stats.delayed, 1)
        self.assertEqual(stats.expired_leases, 1)
        self.assertGreaterEqual(stats.oldest_pending_age, 0)
        self.assertEqual(stats.to_dict()["counts"][DEAD], 0)
        self.assertEqual(finished.id, taken.id)
        self.assertEqual(running.state, RUNNING)

    def test_stats_can_be_scoped_to_a_queue(self):
        queue, _clock = make_queue()
        queue.add(queue="a")
        queue.add(queue="a")
        queue.add(queue="b")
        self.assertEqual(queue.stats(queue="a").counts[PENDING], 2)
        self.assertEqual(queue.stats(queue="b").counts[PENDING], 1)
        self.assertEqual(queue.stats().counts[PENDING], 3)


class JobRecordTest(unittest.TestCase):
    def test_terminal_and_retries_left(self):
        job = Job(id=1, queue="default", payload={}, state=PENDING, attempts=1, max_attempts=3)
        self.assertFalse(job.terminal)
        self.assertEqual(job.retries_left, 2)
        for state in (DONE, DEAD, CANCELLED):
            self.assertTrue(Job(id=1, queue="q", payload={}, state=state).terminal)

    def test_to_dict_is_json_ready(self):
        queue, _clock = make_queue()
        queue.add({"when": "now"}, unique_key="u")
        data = queue.list()[0].to_dict()
        self.assertEqual(json.loads(json.dumps(data))["unique_key"], "u")
        self.assertEqual(data["payload"], {"when": "now"})
        self.assertIn("retries_left", data)


class BackoffTest(unittest.TestCase):
    def test_delays_double_and_cap(self):
        self.assertEqual(compute_delay(1, jitter=0), 2.0)
        self.assertEqual(compute_delay(3, jitter=0), 8.0)
        self.assertEqual(compute_delay(100, jitter=0), 300.0)

    def test_jitter_stays_bounded(self):
        import random

        generator = random.Random(1234)
        raw = compute_delay(5, jitter=0)
        for _ in range(50):
            jittered = compute_delay(5, jitter=0.25, rng=generator)
            self.assertGreaterEqual(jittered, 0.75 * raw)
            self.assertLessEqual(jittered, 1.25 * raw)

    def test_validation(self):
        with self.assertRaises(ValueError):
            compute_delay(0)
        with self.assertRaises(ValueError):
            compute_delay(1, base=0)
        with self.assertRaises(ValueError):
            compute_delay(1, cap=0)
        with self.assertRaises(ValueError):
            compute_delay(1, jitter=1.0)


class ForkTest(unittest.TestCase):
    def test_forking_an_in_memory_queue_is_refused(self):
        queue, _clock = make_queue()
        with self.assertRaises(ValueError):
            queue.fork()

    def test_forked_queues_share_the_database(self):
        with tempfile.TemporaryDirectory() as folder:
            path = os.path.join(folder, "jobs.db")
            first = Queue(path, clock=Clock(), jitter=0.0)
            second = first.fork()
            try:
                job = first.add({"n": 1})
                self.assertEqual(second.take()[0].id, job.id)
                self.assertEqual(first.get(job.id).state, RUNNING)
                self.assertEqual(second.lease_seconds, first.lease_seconds)
            finally:
                first.close()
                second.close()


class WorkerTest(unittest.TestCase):
    def test_single_handler_runs_any_payload(self):
        queue, _clock = make_queue()
        queue.add({"n": 21})
        worker = Worker(queue, {"double": lambda payload: payload["n"] * 2})

        self.assertEqual(worker.run_once(), 1)
        self.assertEqual(queue.list(state=DONE)[0].result, 42)
        self.assertEqual(worker.stats.done, 1)
        self.assertEqual(worker.stats.claimed, 1)

    def test_multi_handler_routes_on_the_task_key(self):
        queue, _clock = make_queue()
        queue.add({"task": "upper", "text": "abc"})
        queue.add({"task": "lower", "text": "ABC"})
        seen = []
        worker = Worker(queue, {
            "upper": lambda payload: seen.append("upper") or payload["text"].upper(),
            "lower": lambda payload: seen.append("lower") or payload["text"].lower(),
        })
        self.assertEqual(worker.run_until_idle(), 2)
        results = sorted(job.result for job in queue.list(state=DONE))
        self.assertEqual(results, ["ABC", "abc"])
        self.assertEqual(sorted(seen), ["lower", "upper"])

    def test_unknown_task_is_dead_lettered_without_burning_retries(self):
        queue, _clock = make_queue()
        queue.add({"task": "missing"}, max_attempts=5)
        worker = Worker(queue, {"known": lambda payload: None, "seen": lambda payload: None})

        worker.run_once()
        job = queue.list()[0]
        self.assertEqual(job.state, DEAD)
        self.assertEqual(job.attempts, 1)
        self.assertIn("missing", job.last_error or "")
        self.assertIn("UnregisteredTask", job.last_error or "")
        self.assertEqual(worker.stats.dead, 1)

    def test_a_single_handler_accepts_any_task_name(self):
        queue, _clock = make_queue()
        queue.add({"task": "whatever", "n": 3})
        Worker(queue, {"only": lambda payload: payload["n"]}).run_once()
        self.assertEqual(queue.list()[0].result, 3)

    def test_payload_without_a_task_key_is_dead_lettered(self):
        queue, _clock = make_queue()
        queue.add({"n": 1})
        Worker(queue, {"a": lambda p: None, "b": lambda p: None}).run_once()
        self.assertEqual(queue.list()[0].state, DEAD)

    def test_permanent_failure_dead_letters_at_once(self):
        queue, _clock = make_queue()
        queue.add(max_attempts=9)

        def explode(payload):
            raise PermanentFailure("schema changed")

        Worker(queue, {"x": explode}).run_once()
        job = queue.list()[0]
        self.assertEqual(job.state, DEAD)
        self.assertIn("schema changed", job.last_error or "")

    def test_a_raised_error_schedules_a_retry(self):
        queue, _clock = make_queue()
        queue.add(max_attempts=3)
        calls = []

        def flaky(payload):
            calls.append(1)
            raise RuntimeError("upstream 503")

        worker = Worker(queue, {"x": flaky})
        worker.run_once()
        pending = queue.list()[0]
        self.assertEqual(pending.state, PENDING)
        self.assertEqual(pending.last_error, "RuntimeError: upstream 503")
        self.assertEqual(worker.stats.retried, 1)
        self.assertEqual(worker.stats.dead, 0)
        self.assertEqual(len(calls), 1)

    def test_an_exhausted_handler_ends_in_the_dead_letter_queue(self):
        queue, clock = make_queue(backoff_base=1.0)
        queue.add(max_attempts=2)
        worker = Worker(queue, {"x": lambda payload: 1 / 0})

        worker.run_once()
        clock.advance(60)
        worker.run_until_idle()

        job = queue.list()[0]
        self.assertEqual(job.state, DEAD)
        self.assertEqual(job.attempts, 2)
        self.assertEqual(worker.stats.dead, 1)
        self.assertEqual(worker.stats.retried, 1)

    def test_worker_without_handlers_dead_letters(self):
        queue, _clock = make_queue()
        queue.add()
        Worker(queue).run_once()
        self.assertEqual(queue.list()[0].state, DEAD)

    def test_register_validates_and_returns_self(self):
        queue, _clock = make_queue()
        worker = Worker(queue)
        self.assertIs(worker.register("task", lambda p: None), worker)
        with self.assertRaises(ValueError):
            worker.register("", lambda p: None)
        with self.assertRaises(TypeError):
            worker.register("nope", "not callable")  # type: ignore[arg-type]

    def test_run_until_idle_drains_the_queue(self):
        queue, _clock = make_queue()
        for n in range(7):
            queue.add({"n": n})
        worker = Worker(queue, {"x": lambda payload: payload["n"]})
        self.assertEqual(worker.run_until_idle(), 7)
        self.assertEqual(worker.run_until_idle(), 0)
        self.assertEqual(len(queue.list(state=DONE, limit=20)), 7)

    def test_stop_ends_the_loop(self):
        queue, _clock = make_queue()
        worker = Worker(queue, {"x": lambda p: None}, poll_interval=0.01)
        worker.stop()
        self.assertTrue(worker.stopping)
        self.assertEqual(worker.run_until_idle(), 0)

    def test_heartbeat_keeps_a_slow_job_alive(self):
        with tempfile.TemporaryDirectory() as folder:
            path = os.path.join(folder, "jobs.db")
            clock = Clock()
            queue = Queue(path, clock=clock, lease_seconds=1.0)
            try:
                queue.add()
                worker = Worker(queue, {"x": lambda payload: None})
                job = queue.take(worker="slow")[0]
                beat = worker._start_heartbeat(queue, job)
                try:
                    clock.advance(0.9)
                    queue.heartbeat(job.id, "slow")
                finally:
                    beat.stop()
                self.assertEqual(queue.get(job.id).state, RUNNING)
            finally:
                queue.close()

    def test_run_forever_with_threads_drains_then_stops(self):
        with tempfile.TemporaryDirectory() as folder:
            path = os.path.join(folder, "jobs.db")
            queue = Queue(path, jitter=0.0)
            try:
                for n in range(15):
                    queue.add({"n": n})

                seen = []
                lock = threading.Lock()

                def handle(payload):
                    with lock:
                        seen.append(payload["n"])

                worker = Worker(queue, {"x": handle}, concurrency=3, poll_interval=0.01)
                runner = threading.Thread(target=worker.run_forever, daemon=True)
                runner.start()

                deadline = time.time() + 20
                while time.time() < deadline:
                    if queue.stats().counts[DONE] >= 15:
                        break
                    time.sleep(0.02)

                worker.stop()
                runner.join(timeout=10)
                self.assertFalse(runner.is_alive())
                self.assertEqual(sorted(seen), list(range(15)))
                self.assertEqual(queue.stats().counts[DONE], 15)
                self.assertEqual(worker.stats.claimed, 15)
            finally:
                queue.close()

    def test_concurrent_workers_process_each_job_once(self):
        with tempfile.TemporaryDirectory() as folder:
            path = os.path.join(folder, "jobs.db")
            setup = Queue(path, jitter=0.0)
            try:
                ids = [setup.add({"n": n}).id for n in range(20)]
            finally:
                setup.close()

            seen = []
            lock = threading.Lock()

            def handle(payload):
                with lock:
                    seen.append(payload["n"])
                return payload["n"] * 2

            errors = []

            def consume() -> None:
                queue = Queue(path, jitter=0.0)
                try:
                    Worker(queue, {"x": handle}, worker_id=threading.current_thread().name) \
                        .run_until_idle()
                except BaseException as exc:  # pragma: no cover - surfaced below
                    errors.append(exc)
                finally:
                    queue.close()

            threads = [threading.Thread(target=consume, name=f"t{i}") for i in range(4)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=30)

            self.assertEqual(errors, [])
            self.assertEqual(sorted(seen), list(range(20)))

            check = Queue(path, jitter=0.0)
            try:
                done = check.list(state=DONE, limit=100)
                self.assertEqual(len(done), 20)
                self.assertEqual(sorted(job.id for job in done), ids)
                self.assertEqual(check.stats().counts[PENDING], 0)
            finally:
                check.close()


class CliTest(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.folder.name, "jobs.db")

    def tearDown(self):
        self.folder.cleanup()

    def run_cli(self, *argv):
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            code = cli.main(list(argv))
        return code, out.getvalue(), err.getvalue()

    def test_init_reports_a_fresh_database(self):
        code, out, _err = self.run_cli("--db", self.db, "init")
        self.assertEqual(code, 0)
        payload = json.loads(out)
        self.assertEqual(payload["counts"][PENDING], 0)
        self.assertTrue(payload["ready"] == 0)

    def test_add_list_and_stats(self):
        code, out, _err = self.run_cli(
            "--db", self.db, "add", "--payload", '{"task": "x"}', "--json"
        )
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out)["duplicate"], False)

        code, out, _err = self.run_cli("--db", self.db, "list", "--json")
        self.assertEqual(code, 0)
        jobs = json.loads(out)["jobs"]
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["payload"], {"task": "x"})

        code, out, _err = self.run_cli("--db", self.db, "stats")
        self.assertEqual(code, 0)
        self.assertIn("pending 1", out)

    def test_add_rejects_invalid_json_with_exit_code_two(self):
        code, _out, err = self.run_cli("--db", self.db, "add", "--payload", "{nope")
        self.assertEqual(code, cli.EXIT_ERROR)
        self.assertIn("not valid JSON", err)

    def test_take_on_an_empty_queue_exits_one(self):
        code, out, _err = self.run_cli("--db", self.db, "take")
        self.assertEqual(code, cli.EXIT_EMPTY)
        self.assertEqual(out, "")

    def test_complete_cycle_through_the_cli(self):
        self.run_cli("--db", self.db, "add", "--payload", '{"n": 2}')
        code, out, _err = self.run_cli("--db", self.db, "take", "--worker", "w1", "--json")
        self.assertEqual(code, 0)
        job = json.loads(out)["jobs"][0]
        self.assertEqual(job["state"], RUNNING)

        code, out, _err = self.run_cli(
            "--db", self.db, "complete", "--id", str(job["id"]), "--worker", "w1",
            "--result", '{"ok": true}', "--json",
        )
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out)["state"], DONE)

        code, out, _err = self.run_cli("--db", self.db, "complete", "--id", str(job["id"]),
                                       "--worker", "w1")
        self.assertEqual(code, cli.EXIT_LEASE_LOST)

    def test_cancel_and_requeue_contract(self):
        self.run_cli("--db", self.db, "add")
        code, _out, _err = self.run_cli("--db", self.db, "cancel", "--id", "1")
        self.assertEqual(code, 0)

        code, _out, err = self.run_cli("--db", self.db, "requeue", "--id", "1")
        self.assertEqual(code, cli.EXIT_BAD_STATE)
        self.assertIn("only dead jobs requeue", err)

        code, _out, _err = self.run_cli("--db", self.db, "requeue", "--id", "999")
        self.assertEqual(code, cli.EXIT_BAD_STATE)

    def test_purge_reports_a_count(self):
        self.run_cli("--db", self.db, "add")
        self.run_cli("--db", self.db, "take", "--worker", "w1")
        self.run_cli("--db", self.db, "complete", "--id", "1", "--worker", "w1")
        code, out, _err = self.run_cli("--db", self.db, "purge", "--json")
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out)["deleted"], 1)

    def test_work_command_runs_a_dotted_handler(self):
        self.run_cli("--db", self.db, "add", "--payload", json.dumps({"task": "x"}))
        code, out, _err = self.run_cli(
            "--db", self.db, "work", "--handler", "queuelite.job:encode_payload"
        )
        self.assertEqual(code, 0)
        self.assertIn("done", out)

        code, out, _err = self.run_cli("--db", self.db, "list", "--json")
        job = json.loads(out)["jobs"][0]
        self.assertEqual(job["state"], DONE)
        self.assertEqual(json.loads(job["result"]), {"task": "x"})

    def test_work_reports_a_handler_error_and_keeps_the_job_pending(self):
        self.run_cli("--db", self.db, "add")
        code, out, _err = self.run_cli(
            "--db", self.db, "work", "--handler", "queuelite.cli:load_handler",
            "--json",
        )
        self.assertEqual(code, 0)
        self.assertIn("ValueError", out)
        self.assertIn('"state": "pending"', out)

        code, out, _err = self.run_cli("--db", self.db, "list", "--json")
        self.assertEqual(json.loads(out)["jobs"][0]["state"], PENDING)

    def test_load_handler_validates_the_spec(self):
        with self.assertRaises(ValueError):
            cli.load_handler("no_colon")
        with self.assertRaises(ImportError):
            cli.load_handler("queuelite.nope:thing")
        with self.assertRaises(ValueError):
            cli.load_handler("queuelite.job:PENDING")

    def test_reap_reports_recovered_jobs(self):
        self.run_cli("--db", self.db, "--lease", "1", "add")
        self.run_cli("--db", self.db, "--lease", "1", "take", "--worker", "w1")
        time.sleep(1.1)
        code, out, _err = self.run_cli("--db", self.db, "reap", "--json")
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out)["recovered"], [1])


class ModuleTest(unittest.TestCase):
    def test_package_metadata_and_exports(self):
        import queuelite

        self.assertEqual(queuelite.__version__, "1.0.0")
        for name in ("Queue", "Worker", "Job", "compute_delay"):
            self.assertTrue(hasattr(queuelite, name))

    def test_encode_payload_is_stable_and_sorted(self):
        self.assertEqual(encode_payload({"b": 1, "a": 2}), '{"a":2,"b":1}')
        self.assertEqual(encode_payload(None), "{}")
        with self.assertRaises(ValueError):
            encode_payload(object())


if __name__ == "__main__":
    unittest.main()
