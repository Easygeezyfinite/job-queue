"""Tests for JobQueue.submit.

TestSubmitWithFakeRedis uses the in-memory FakeRedis from fake_redis.py so it runs
anywhere. TestSubmitWithRealRedis needs a Redis server on localhost:6379 and the
redis package, and is skipped when either is missing.
"""

import json
import unittest
import uuid

from job_queue import Job, JobQueue

from fake_redis import FakeRedis
from real_redis import real_redis


class TestSubmitWithFakeRedis(unittest.TestCase):
    def setUp(self):
        self.fake_redis = FakeRedis()
        self.queue = JobQueue(self.fake_redis, queue_name="emails")

    def test_submit_returns_the_job(self):
        job = self.queue.submit("send_email", {"to": "a@example.com"})
        self.assertIsInstance(job, Job)
        self.assertEqual(job.job_type, "send_email")
        self.assertEqual(job.payload, {"to": "a@example.com"})

    def test_submit_pushes_onto_the_head_of_the_list(self):
        self.queue.submit("first")
        self.queue.submit("second")
        head = json.loads(self.fake_redis.lists["jobqueue:emails"][0])
        self.assertEqual(head["job_type"], "second")

    def test_size_counts_submitted_jobs(self):
        self.assertEqual(self.queue.size(), 0)
        for _ in range(3):
            self.queue.submit("noop")
        self.assertEqual(self.queue.size(), 3)

    def test_pending_jobs_are_oldest_first(self):
        submitted = [self.queue.submit("job", {"n": n}) for n in range(5)]
        self.assertEqual(self.queue.pending_jobs(), submitted)

    def test_stored_job_matches_what_submit_returned(self):
        job = self.queue.submit("resize_image", {"width": 200})
        stored = Job.from_json(self.fake_redis.lists["jobqueue:emails"][0])
        self.assertEqual(stored, job)

    def test_queues_with_different_names_are_separate(self):
        other_queue = JobQueue(self.fake_redis, queue_name="images")
        self.queue.submit("send_email")
        other_queue.submit("resize_image")
        other_queue.submit("resize_image")
        self.assertEqual(self.queue.size(), 1)
        self.assertEqual(other_queue.size(), 2)

    def test_bad_job_is_not_pushed(self):
        # Validation happens before LPUSH, so nothing half-valid lands on the queue.
        with self.assertRaises(ValueError):
            self.queue.submit("noop", {"ids": {1, 2}})
        self.assertEqual(self.queue.size(), 0)

    def test_empty_queue_name_is_rejected(self):
        with self.assertRaises(ValueError):
            JobQueue(self.fake_redis, queue_name="")


@unittest.skipIf(real_redis is None, "no Redis server on localhost:6379 (or redis package not installed)")
class TestSubmitWithRealRedis(unittest.TestCase):
    def setUp(self):
        # Random prefix so test keys never collide with anything else in Redis.
        self.key_prefix = f"test_jobqueue_{uuid.uuid4().hex}"
        self.queue = JobQueue(real_redis, queue_name="tests", key_prefix=self.key_prefix)

    def tearDown(self):
        real_redis.delete(self.queue.key)

    def test_submit_adds_to_redis_list(self):
        self.queue.submit("send_email", {"to": "a@example.com"})
        self.assertEqual(real_redis.llen(self.queue.key), 1)

    def test_jobs_come_back_oldest_first(self):
        submitted = [self.queue.submit("job", {"n": n}) for n in range(3)]
        self.assertEqual(self.queue.pending_jobs(), submitted)

    def test_rpop_gives_the_oldest_job(self):
        # This is what the worker will do (with the blocking version, BRPOP).
        first = self.queue.submit("first")
        self.queue.submit("second")
        self.assertEqual(Job.from_json(real_redis.rpop(self.queue.key)), first)


if __name__ == "__main__":
    unittest.main()
