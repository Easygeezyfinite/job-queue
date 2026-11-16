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
- [x] Future work write-up (see [Future work](#future-work))

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
.github/workflows/ci.yml   runs the tests against real Redis on every push
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

On GitHub, `.github/workflows/ci.yml` runs the suite on every push against a real
Redis 7 service container, and fails the build if any test is skipped.

## Future work

The project stops here on purpose. These are the pieces a production queue would add,
roughly in the order I'd build them, and how each one would fit the current design.

### 1. At-least-once delivery

Fixes the limitation above. Instead of `BRPOP`, a worker would use
`BLMOVE jobqueue:<name> jobqueue:<name>:processing:<worker id> RIGHT LEFT`, which takes
the job and parks it on a per-worker list in one atomic step. The worker deletes it
from that list (`LREM`) only after the handler finishes. If the worker dies, the job is
still sitting in its processing list and can be recovered.

The trade-off is that a job can now run twice (the worker finishes the handler but
crashes before the `LREM`), so handlers need to be idempotent. Sending the same email
twice is a real bug, so a handler would record "job X done" somewhere before or along
with its side effect.

### 2. Retries with backoff

Right now a failing job goes straight to the failed list. A `Job` would get an
`attempts` count and a `max_attempts` limit. On failure the worker bumps the count and,
if there are attempts left, re-queues the job with a delay that doubles each time
(for example 1s, 2s, 4s, 8s, plus a little random jitter so many failed jobs don't all
retry in the same instant). Only after the last attempt does it land on the failed
list. Delayed re-queueing depends on scheduling (next item).

### 3. Scheduled and delayed jobs

A Redis sorted set, `jobqueue:<name>:scheduled`, with the run time as the score. A
small loop (or each worker between jobs) runs `ZRANGEBYSCORE ... -inf <now>` to find
jobs that are due and moves them onto the main list. Moving them has to be atomic
(a Lua script or `MULTI`) so two workers can't both move the same job and run it
twice. Retries from item 2 would use the same sorted set.

### 4. Multi-worker coordination

`BRPOP` already makes sure two workers never get the same job, and the tests check
that with three workers. What's missing is the cleanup side of item 1:

- Each worker writes a heartbeat key with a short TTL (`SET worker:<id> alive EX 30`)
  every few seconds.
- A "reaper" checks the processing lists. If a worker's heartbeat has expired, its
  in-flight jobs go back on the main queue.
- Graceful shutdown: on SIGTERM, finish the current job, then exit, instead of only
  checking `stop()` between jobs.

### 5. Monitoring

The numbers I'd want on a dashboard, most of them one Redis command away:

| Metric | Where it comes from |
|---|---|
| Queue depth | `LLEN jobqueue:<name>` |
| Failed jobs | `LLEN jobqueue:<name>:failed` |
| Jobs in flight | `LLEN` of each processing list |
| Live workers | count of unexpired heartbeat keys |
| Job latency (submit to start) | `created_at` vs. the time the worker picks it up |
| Handler duration and error rate | timed and counted in the worker loop |

These would be exposed as Prometheus metrics or a small `/stats` endpoint, with an
alert when queue depth keeps growing (workers can't keep up) or the failed list grows
quickly. A small CLI to list failed jobs and re-queue them by id would also help.

### Not planned

- Job priorities (would need one list per priority, or a sorted set instead of a list).
- Exactly-once processing. Across a network that means idempotent handlers plus
  at-least-once delivery (item 1), not a stronger queue.
