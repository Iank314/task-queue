# Distributed Task Queue

A distributed task queue built on PostgreSQL, with an async Python broker and workers that use `SELECT FOR UPDATE SKIP LOCKED` for safe concurrent job claiming.

## Architecture

```
                         +-------------+
                         |   Client    |
                         +------+------+
                                |
                           HTTP REST API
                                |
                         +------v------+
                         |   Broker    |
                         |  (aiohttp)  |
                         +------+------+
                                |
                          INSERT / SELECT
                                |
                         +------v------+
                         | PostgreSQL  |
                         |  (job store)|
                         +------+------+
                                ^
               SELECT FOR UPDATE SKIP LOCKED
                                |
              +-----------------+-----------------+
              |                 |                 |
        +-----+-----+    +-----+-----+    +-----+-----+
        |  Worker 1  |    |  Worker 2  |    |  Worker 3  |
        +------------+    +------------+    +------------+
```

Clients submit jobs via the broker's REST API. Jobs are written to PostgreSQL. Workers independently poll for pending jobs, claim them atomically, execute handlers, and write results back.

## Features

- **Priority-based job submission** -- jobs are processed in priority order.
- **Concurrent claiming with no double-processing** -- multiple workers compete for jobs safely using PostgreSQL row-level locking.
- **Exponential backoff retry** -- failed jobs are retried up to 3 times with a delay of 2^attempts seconds.
- **Crash recovery (lease + heartbeat + reaper)** -- if a worker dies mid-job, its lease expires and the job is automatically returned to the queue instead of being stranded in `running`.
- **Job cancellation** -- pending jobs can be cancelled before a worker claims them.
- **Dead letter queue** -- permanently failed jobs are moved to a separate table for inspection, retry, or discard.
- **Metrics endpoint** -- returns job counts grouped by status, including dead letter count.
- **Dashboard UI** -- web interface for monitoring queue health, managing jobs, and retrying/discarding dead letter jobs.
- **Integration tests** -- tests run against real PostgreSQL, including concurrent claiming proof for SKIP LOCKED.
- **CI via GitHub Actions** -- automated test suite on every push.

## How SKIP LOCKED Prevents Double-Processing

Workers claim jobs with a query like:

```sql
SELECT * FROM jobs
WHERE status = 'pending'
ORDER BY priority DESC, created_at ASC
FOR UPDATE SKIP LOCKED
LIMIT 1;
```

`FOR UPDATE` locks the selected row for the duration of the transaction. `SKIP LOCKED` tells other concurrent transactions to silently skip any rows that are already locked rather than waiting. This means two workers executing the same query at the same instant will always receive different rows -- no double-processing, no contention, no advisory locks needed.

## Crash Recovery: Leases, Heartbeats, and the Reaper

Claiming a job sets its status to `running`. But what if that worker is killed
(OOM, crash, network partition) before it finishes? Without recovery the job is
stranded in `running` forever -- no other worker will ever touch it. This is the
classic at-least-once delivery problem, and the queue solves it the same way SQS
visibility timeouts and Kafka consumer leases do:

1. **Lease.** When a worker claims a job it stamps a `lease_expires_at` deadline
   on the row (default 30s out). The lease is a promise: *"I'll finish this, or
   renew the promise, before this time."*

2. **Heartbeat.** A real job may take longer than one lease. While the handler
   runs, the worker renews the lease on a timer (every `LEASE_DURATION / 3`) from
   a separate DB session, pushing the deadline forward. A healthy worker's lease
   therefore never expires, no matter how long the job legitimately takes.

3. **Reaper.** Every worker also runs a background loop that sweeps for jobs
   where `status = 'running' AND lease_expires_at < now()` -- the tell-tale sign
   of a worker that stopped heartbeating. It reclaims them using the *same*
   `FOR UPDATE SKIP LOCKED` query as claiming, so multiple reapers never fight
   over the same row.

A dead worker is treated as just another failed attempt: the reaped job flows
through the normal retry/backoff path, and only dead-letters once its attempts
are exhausted. Because the attempt is counted at *claim* time (not on success), a
"poison" job that keeps killing workers is still bounded by `max_attempts` and
can't be retried forever.

A crash is detected within roughly `LEASE_DURATION + REAPER_INTERVAL` seconds.
Both are tunable via environment variables (see `.env.example`):

| Variable                  | Default | Meaning                                            |
|---------------------------|---------|----------------------------------------------------|
| `LEASE_DURATION_SECONDS`  | `30`    | How long a claim is valid before it must be renewed |
| `REAPER_INTERVAL_SECONDS` | `15`    | How often each worker sweeps for expired leases     |

## Quick Start

Prerequisites: Docker and Docker Compose.

```bash
git clone https://github.com/Iank314/task-queue.git
cd task-queue
docker compose up --build
```

This starts PostgreSQL, the broker, and three worker replicas. Once running:

- **Dashboard:** http://localhost:8000/dashboard
- **API:** http://localhost:8000

## API Reference

### Submit a Job

```bash
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{"type": "process_data", "payload": {"key": "value"}, "priority": 5}'
```

### List Jobs

```bash
# All jobs
curl http://localhost:8000/jobs

# Filter by status
curl http://localhost:8000/jobs?status=pending
```

### Get Job Details

```bash
curl http://localhost:8000/jobs/{id}
```

### Cancel a Pending Job

```bash
curl -X DELETE http://localhost:8000/jobs/{id}
```

Returns an error if the job is already claimed or completed.

### Get Metrics

```bash
curl http://localhost:8000/metrics
```

Returns job counts by status:

```json
{"pending": 12, "running": 3, "completed": 48, "cancelled": 1, "dead_letter": 2}
```

### Dead Letter Queue

Jobs that exhaust all retry attempts are moved to a dead letter table. They can be inspected, retried, or permanently discarded.

```bash
# List dead letter jobs
curl http://localhost:8000/dead-letter

# Retry a dead letter job (resubmits as a new pending job)
curl -X POST http://localhost:8000/dead-letter/{id}/retry

# Permanently discard a dead letter job
curl -X DELETE http://localhost:8000/dead-letter/{id}
```

## Project Structure

```
task-queue/
  broker/              REST API (aiohttp)
  worker/              Poll loop and job handlers
  dashboard/           Web UI
  migrations/          SQL schema (jobs + dead_letter_jobs)
  tests/               Unit and integration test suite
  docker-compose.yml
  Dockerfile.broker
  Dockerfile.worker
  .github/workflows/ci.yml
```

## Tech Stack

| Component  | Technology                      |
|------------|---------------------------------|
| Language   | Python 3.12                     |
| Broker     | aiohttp                         |
| ORM        | SQLAlchemy (async)              |
| DB Driver  | asyncpg                         |
| Database   | PostgreSQL 16                   |
| Containers | Docker, Docker Compose          |
| CI         | GitHub Actions                  |
