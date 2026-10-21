"""A worker that pulls jobs off a JobQueue and runs them one at a time.

Each job type maps to a handler function that takes the job's payload:

    handlers = {"send_email": send_email, "resize_image": resize_image}
    Worker(queue, handlers).run()

If a handler raises, or a job has a type nobody handles, the job is moved to the
queue's failed list and the worker carries on with the next one. One bad job
shouldn't take the whole worker down.

Known limitation: BRPOP removes the job from Redis before the handler runs, so if
the worker process dies halfway through a job, that job is gone. That makes this
"at most once" delivery. See the README for how a production queue avoids this.
"""

import logging

from .job import Job

logger = logging.getLogger(__name__)


class Worker:
    def __init__(self, queue, handlers, poll_timeout_seconds=1):
        if not handlers:
            raise ValueError("a worker needs at least one handler")
        self.queue = queue
        self.handlers = dict(handlers)
        self.poll_timeout_seconds = poll_timeout_seconds
        self.should_stop = False
        self.processed_count = 0
        self.failed_count = 0
        # Every job taken off the queue, whether it succeeded or failed.
        self.jobs_taken = 0

    def stop(self):
        """Ask the worker to finish its current job and exit. Safe to call from another thread."""
        self.should_stop = True

    def run_once(self):
        """Wait for one job and process it.

        Returns the Job if one was handled successfully, or None if the wait timed
        out or the job failed.
        """
        raw_job = self.queue.fetch_next(timeout_seconds=self.poll_timeout_seconds)
        if raw_job is None:
            return None
        self.jobs_taken += 1

        try:
            job = Job.from_json(raw_job)
        except ValueError as error:
            self._fail(raw_job, f"could not parse job: {error}")
            return None

        handler = self.handlers.get(job.job_type)
        if handler is None:
            self._fail(raw_job, f"no handler for job type {job.job_type!r}")
            return None

        try:
            handler(job.payload)
        except Exception as error:
            # Catching everything is on purpose here: whatever the handler does wrong,
            # the worker should record it and move on to the next job.
            logger.exception("job %s (%s) failed", job.job_id, job.job_type)
            self._fail(raw_job, f"{type(error).__name__}: {error}")
            return None

        self.processed_count += 1
        return job

    def run(self, max_jobs=None):
        """Keep processing jobs until stop() is called (or max_jobs have been attempted).

        The BRPOP timeout means the loop wakes up at least every poll_timeout_seconds
        even when the queue is empty, which is when it notices a stop request.
        max_jobs is mostly for tests and scripts that want a worker to drain a fixed batch.
        """
        taken_at_start = self.jobs_taken
        while not self.should_stop:
            if max_jobs is not None and self.jobs_taken - taken_at_start >= max_jobs:
                break
            self.run_once()

    def _fail(self, raw_job, reason):
        self.failed_count += 1
        logger.warning("moving job to failed list: %s", reason)
        self.queue.record_failure(raw_job, reason)
