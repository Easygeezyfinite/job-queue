"""Tests for the worker loop: FIFO order, blocking, stopping, and failed jobs.

The fake-Redis tests run anywhere. TestWorkerWithRealRedis needs a Redis server on
localhost:6379 and the redis package, and is skipped when either is missing.
"""

import json
import threading
import time
import unittest
import uuid

from job_queue import JobQueue, Worker

from fake_redis import FakeRedis
from real_redis import real_redis


class RecordingHandler:
    """A handler that just remembers every payload it was given, in order."""

    def __init__(self):
        self.payloads = []
        self.lock = threading.Lock()

    def __call__(self, payload):
        with self.lock:
            self.payloads.append(payload)


def broken_handler(payload):
    raise RuntimeError("smtp server down")


class TestWorkerWithFakeRedis(unittest.TestCase):
    def setUp(self):
        self.fake_redis = FakeRedis()
        self.queue = JobQueue(self.fake_redis, queue_name="tasks")
        self.handler = RecordingHandler()
        # Short poll timeout so tests that hit an empty queue don't sit around.
        self.worker = Worker(self.queue, {"task": self.handler}, poll_timeout_seconds=0.05)

    def test_jobs_are_processed_in_submit_order(self):
        for n in range(10):
            self.queue.submit("task", {"n": n})
        self.worker.run(max_jobs=10)
        self.assertEqual([p["n"] for p in self.handler.payloads], list(range(10)))
        self.assertEqual(self.queue.size(), 0)

    def test_fifo_holds_when_submits_and_pops_interleave(self):
        self.queue.submit("task", {"name": "A"})
        self.queue.submit("task", {"name": "B"})
        self.worker.run_once()
        self.queue.submit("task", {"name": "C"})
        self.worker.run_once()
        self.worker.run_once()
        self.assertEqual([p["name"] for p in self.handler.payloads], ["A", "B", "C"])

    def test_run_once_returns_the_job_it_handled(self):
        submitted = self.queue.submit("task", {"n": 1})
        self.assertEqual(self.worker.run_once(), submitted)
        self.assertEqual(self.worker.processed_count, 1)

    def test_run_once_on_empty_queue_times_out_with_none(self):
        started = time.monotonic()
        self.assertIsNone(self.worker.run_once())
        self.assertGreaterEqual(time.monotonic() - started, 0.04)
        self.assertEqual(self.worker.jobs_taken, 0)

    def test_worker_waits_for_a_job_submitted_later(self):
        self.worker.poll_timeout_seconds = 2
        submitter = threading.Timer(0.1, lambda: self.queue.submit("task", {"n": 42}))
        submitter.start()
        job = self.worker.run_once()
        submitter.join()
        self.assertIsNotNone(job)
        self.assertEqual(job.payload, {"n": 42})

    def test_stop_from_another_thread_ends_run(self):
        worker_thread = threading.Thread(target=self.worker.run)
        worker_thread.start()
        time.sleep(0.1)
        self.worker.stop()
        # The loop should notice within one poll timeout (0.05s), so 1s is plenty.
        worker_thread.join(timeout=1)
        self.assertFalse(worker_thread.is_alive())

    def test_failing_handler_moves_job_to_failed_list_and_worker_continues(self):
        worker = Worker(
            self.queue,
            {"send_email": broken_handler, "task": self.handler},
            poll_timeout_seconds=0.05,
        )
        self.queue.submit("send_email", {"to": "a@example.com"})
        self.queue.submit("task", {"n": 1})
        with self.assertLogs("job_queue.worker", level="WARNING"):
            worker.run(max_jobs=2)

        self.assertEqual(self.handler.payloads, [{"n": 1}])
        self.assertEqual(worker.failed_count, 1)
        self.assertEqual(self.queue.failed_count(), 1)
        failed_entry = json.loads(self.fake_redis.lists["jobqueue:tasks:failed"][0])
        self.assertIn("smtp server down", failed_entry["reason"])
        self.assertIn("send_email", failed_entry["raw_job"])

    def test_unknown_job_type_goes_to_failed_list(self):
        self.queue.submit("resize_image", {"width": 100})
        with self.assertLogs("job_queue.worker", level="WARNING"):
            self.assertIsNone(self.worker.run_once())
        self.assertEqual(self.queue.failed_count(), 1)
        failed_entry = json.loads(self.fake_redis.lists["jobqueue:tasks:failed"][0])
        self.assertIn("no handler", failed_entry["reason"])

    def test_corrupt_entry_goes_to_failed_list_without_crashing(self):
        # Something other than our code pushed garbage onto the list.
        self.fake_redis.lpush(self.queue.key, "this is not json")
        self.queue.submit("task", {"n": 1})
        with self.assertLogs("job_queue.worker", level="WARNING"):
            self.worker.run(max_jobs=2)
        self.assertEqual(self.queue.failed_count(), 1)
        self.assertEqual(self.handler.payloads, [{"n": 1}])

    def test_worker_needs_at_least_one_handler(self):
        with self.assertRaises(ValueError):
            Worker(self.queue, {})

    def test_zero_timeout_is_rejected(self):
        # 0 means "block forever" to BRPOP, which would make stop() useless.
        with self.assertRaises(ValueError):
            self.queue.fetch_next(timeout_seconds=0)


@unittest.skipIf(real_redis is None, "no Redis server on localhost:6379 (or redis package not installed)")
class TestWorkerWithRealRedis(unittest.TestCase):
    def setUp(self):
        self.key_prefix = f"test_jobqueue_{uuid.uuid4().hex}"
        self.queue = JobQueue(real_redis, queue_name="tests", key_prefix=self.key_prefix)
        self.handler = RecordingHandler()

    def tearDown(self):
        real_redis.delete(self.queue.key, self.queue.failed_key)

    def test_jobs_are_processed_in_submit_order(self):
        for n in range(20):
            self.queue.submit("task", {"n": n})
        worker = Worker(self.queue, {"task": self.handler})
        worker.run(max_jobs=20)
        self.assertEqual([p["n"] for p in self.handler.payloads], list(range(20)))

    def test_worker_waits_for_a_job_submitted_later(self):
        worker = Worker(self.queue, {"task": self.handler}, poll_timeout_seconds=2)
        submitter = threading.Timer(0.2, lambda: self.queue.submit("task", {"n": 7}))
        submitter.start()
        job = worker.run_once()
        submitter.join()
        self.assertEqual(job.payload, {"n": 7})

    def test_failed_job_lands_on_failed_list_in_redis(self):
        worker = Worker(self.queue, {"task": broken_handler})
        self.queue.submit("task", {"n": 1})
        with self.assertLogs("job_queue.worker", level="WARNING"):
            worker.run_once()
        self.assertEqual(real_redis.llen(self.queue.failed_key), 1)

    def test_several_workers_never_handle_the_same_job_twice(self):
        # BRPOP hands each job to exactly one waiting client, so with three
        # workers racing on one list every job should still be handled once.
        total_jobs = 150
        submitted_numbers = {self.queue.submit("task", {"n": n}).payload["n"] for n in range(total_jobs)}
        workers = [Worker(self.queue, {"task": self.handler}, poll_timeout_seconds=1) for _ in range(3)]
        threads = [threading.Thread(target=worker.run) for worker in workers]
        for thread in threads:
            thread.start()

        deadline = time.monotonic() + 10
        while len(self.handler.payloads) < total_jobs and time.monotonic() < deadline:
            time.sleep(0.05)
        for worker in workers:
            worker.stop()
        for thread in threads:
            thread.join(timeout=5)

        handled_numbers = [p["n"] for p in self.handler.payloads]
        self.assertEqual(len(handled_numbers), total_jobs)
        self.assertEqual(set(handled_numbers), submitted_numbers)


if __name__ == "__main__":
    unittest.main()
