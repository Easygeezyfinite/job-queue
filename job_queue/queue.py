"""A FIFO job queue stored in a Redis list.

Producers call submit(), which does LPUSH: the new job goes on the left (head)
of the list. Workers take jobs from the right (tail) with BRPOP, so the
oldest job always comes out first:

    submit A, B, C  ->  list is [C, B, A]  ->  worker gets A, then B, then C

LPUSH is a single Redis command, so two producers submitting at the same time
can't lose or corrupt each other's jobs. Redis just applies them one after the other.
"""

import json
import time

from .job import Job


class JobQueue:
    def __init__(self, redis_client, queue_name="default", key_prefix="jobqueue"):
        if not queue_name:
            raise ValueError("queue_name must not be empty")
        self.redis = redis_client
        self.queue_name = queue_name
        self.key = f"{key_prefix}:{queue_name}"
        self.failed_key = f"{self.key}:failed"

    def submit(self, job_type, payload=None):
        """Create a job, push it onto the queue and return it (so the caller has the id)."""
        job = Job(job_type=job_type, payload=payload if payload is not None else {})
        self.redis.lpush(self.key, job.to_json())
        return job

    def fetch_next(self, timeout_seconds=1):
        """Take the oldest job off the queue, waiting up to timeout_seconds for one.

        Returns the raw job string (bytes from the real client), or None if nothing
        arrived in time. Parsing is left to the worker so a corrupt entry can be
        handled there instead of blowing up here.

        BRPOP removes the job and hands it to exactly one caller, even with several
        workers waiting on the same list. A timeout of 0 would mean "wait forever",
        which would stop a worker from ever checking whether it was asked to shut
        down, so it isn't allowed.
        """
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        result = self.redis.brpop([self.key], timeout=timeout_seconds)
        if result is None:
            return None
        _key, raw_job = result
        return raw_job

    def record_failure(self, raw_job, reason):
        """Keep jobs that couldn't be processed on a separate list for a human to look at."""
        if isinstance(raw_job, bytes):
            raw_job = raw_job.decode("utf-8", errors="replace")
        entry = json.dumps({"raw_job": raw_job, "reason": reason, "failed_at": time.time()})
        self.redis.lpush(self.failed_key, entry)

    def failed_count(self):
        return self.redis.llen(self.failed_key)

    def size(self):
        return self.redis.llen(self.key)

    def pending_jobs(self):
        """All waiting jobs, oldest first. Handy for debugging; workers don't use this."""
        # LRANGE returns head to tail (newest first), so reverse it.
        raw_jobs = self.redis.lrange(self.key, 0, -1)
        return [Job.from_json(raw) for raw in reversed(raw_jobs)]
