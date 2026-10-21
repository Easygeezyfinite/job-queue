# Job Queue

A small background job queue in Python, backed by a Redis list. Producers submit jobs
(a type name plus a JSON payload), and workers take them off the queue in the order
they were submitted.

This is a deliberately small project. The goal is to get the core idea right (a
reliable FIFO hand-off between producers and workers through Redis) and write down
what a production queue would need on top of that, rather than build all of it.

## Status

- [x] Job model and `submit` (LPUSH)
- [x] Worker loop (BRPOP) and FIFO tests
- [ ] Future work write-up

## How it works

Each queue is one Redis list, stored at the key `jobqueue:<queue name>`.

- `submit` does `LPUSH`, which adds the job to the left (head) of the list.
- A worker does `BRPOP`, which takes from the right (tail) and waits if the list is empty.

Pushing on one end and popping from the other makes the list a FIFO queue:

```
submit A        [A]
submit B        [B, A]
submit C        [C, B, A]
worker pops     A   (list is now [C, B])
```

`LPUSH` is a single Redis command, so producers submitting at the same time can't
overwrite each other. Redis runs commands one at a time.

Jobs are stored as JSON:

```json
{"job_id": "3f2a...", "job_type": "send_email", "payload": {"to": "a@example.com"}, "created_at": 1760900000.0}
```

The payload is checked when the job is created, so something that can't be turned
into JSON (like a Python `set`) is rejected at submit time instead of breaking a
worker later.

## The worker

A `Worker` maps job types to handler functions and runs jobs one at a time:

1. `BRPOP` on the queue with a short timeout (1 second by default).
2. If nothing arrived, loop again. This is also where it notices `stop()`.
3. Parse the job and call the handler for its type with the payload.
4. If the handler raises, the job has an unknown type, or the entry isn't valid job
   JSON, the job goes onto a separate list, `jobqueue:<queue name>:failed`, with the
   reason, and the worker moves on to the next job.

`BRPOP` gives each job to exactly one waiting client, so several workers can share a
queue without handling the same job twice.

The timeout is never 0 (which means "wait forever" to `BRPOP`). Otherwise a worker
sitting on an empty queue would never get a chance to check whether it should stop.

### Known limitation: at-most-once delivery

`BRPOP` removes the job from Redis before the handler runs. If the worker process
crashes in the middle of a job, that job is lost. A production queue would move the
job to a per-worker "processing" list atomically (`BLMOVE`) and only delete it when
the work is done. This project stops short of that on purpose.

## Usage

```python
import redis
from job_queue import JobQueue

queue = JobQueue(redis.Redis(), queue_name="emails")
job = queue.submit("send_email", {"to": "a@example.com"})
print(job.job_id, queue.size())
```

Running a worker:

```python
import redis
from job_queue import JobQueue, Worker


def send_email(payload):
    print("sending email to", payload["to"])


queue = JobQueue(redis.Redis(), queue_name="emails")
worker = Worker(queue, {"send_email": send_email})
worker.run()
```

`worker.run()` keeps going until `worker.stop()` is called (for example from another
thread) or the process is stopped with Ctrl+C. `worker.run(max_jobs=10)` stops after
ten jobs.

## Project layout

```
job_queue/
    job.py      Job dataclass, JSON encoding and validation
    queue.py    JobQueue: submit, fetch_next (BRPOP), failed list
    worker.py   Worker: handler lookup, run loop, stop, failure handling
tests/
    fake_redis.py       in-memory stand-in for the Redis list commands
    real_redis.py       connects to a local Redis for the integration tests
    test_job.py
    test_queue_submit.py
    test_worker.py
```

## Running the tests

Requires Python 3.10+ and, for the real-Redis tests, a Redis server on `localhost:6379`.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 -m unittest discover -s tests -v
```

Most tests use a small in-memory stand-in for Redis and run anywhere. The tests that
talk to a real server are skipped if Redis or the `redis` package isn't available.
`REDIS_HOST` and `REDIS_PORT` override the address.

On macOS I run Redis in Docker:

```bash
docker run -d --name redis -p 6379:6379 --restart unless-stopped redis:7
docker exec redis redis-cli ping
```

If the `redis` container already exists, start it with `docker start redis` instead.
