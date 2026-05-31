import asyncio
from datetime import datetime, timezone, timedelta
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from contextlib import asynccontextmanager
from broker.models import Base, Job, DeadLetterJob
from worker.handlers import HANDLERS
import os

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://user:password@postgres/taskqueue")

# Crash-recovery tuning. The lease is how long a claimed job may run before a
# worker must renew it; if the lease lapses the worker is presumed dead and the
# reaper reclaims the job. Heartbeats renew the lease well before it expires so
# long-running jobs survive, while a crashed worker is detected within roughly
# LEASE_DURATION + REAPER_INTERVAL seconds.
LEASE_DURATION_SECONDS = int(os.getenv("LEASE_DURATION_SECONDS", "30"))
HEARTBEAT_INTERVAL_SECONDS = int(
    os.getenv("HEARTBEAT_INTERVAL_SECONDS", str(max(1, LEASE_DURATION_SECONDS // 3)))
)
REAPER_INTERVAL_SECONDS = int(os.getenv("REAPER_INTERVAL_SECONDS", "15"))

engine = create_async_engine(DATABASE_URL)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


@asynccontextmanager
async def get_session():
    async with AsyncSessionLocal() as session:
        yield session


async def claim_job(session) -> Job | None:
    result = await session.execute(
        select(Job)
        .where(Job.status == "pending")
        .where(Job.run_after <= datetime.now(timezone.utc))
        .order_by(Job.priority.desc(), Job.created_at.asc())
        .limit(1)
        .with_for_update(skip_locked=True)
    )
    job = result.scalar_one_or_none()
    if job:
        now = datetime.now(timezone.utc)
        job.status = "running"
        job.attempts += 1
        job.lease_expires_at = now + timedelta(seconds=LEASE_DURATION_SECONDS)
        job.updated_at = now
        await session.commit()
    return job


async def complete_job(session, job: Job, result: dict):
    job.status = "completed"
    job.result = result
    job.lease_expires_at = None
    job.updated_at = datetime.now(timezone.utc)
    await session.commit()


async def fail_job(session, job: Job, error: str):
    job.error = error
    if job.attempts >= job.max_attempts:
        dead = DeadLetterJob(
            original_job_id=job.id,
            type=job.type,
            payload=job.payload,
            priority=job.priority,
            attempts=job.attempts,
            error=error,
            created_at=job.created_at,
        )
        session.add(dead)
        await session.delete(job)
        await session.commit()
        print(f"Job {job.id} moved to dead letter queue")
    else:
        job.status = "pending"
        delay = 2 ** job.attempts  # 2s, 4s, 8s
        job.run_after = datetime.now(timezone.utc) + timedelta(seconds=delay)
        job.lease_expires_at = None
        job.updated_at = datetime.now(timezone.utc)
        await session.commit()


async def extend_lease(session, job_id) -> None:
    """Push a running job's lease deadline forward (a heartbeat).

    Guarded on status == 'running' so it can never resurrect the lease of a job
    that has already finished or been reclaimed -- in those cases it's a no-op.
    """
    now = datetime.now(timezone.utc)
    await session.execute(
        update(Job)
        .where(Job.id == job_id)
        .where(Job.status == "running")
        .values(
            lease_expires_at=now + timedelta(seconds=LEASE_DURATION_SECONDS),
            updated_at=now,
        )
    )
    await session.commit()


async def _heartbeat(job_id, stop: asyncio.Event) -> None:
    """Renew a job's lease every HEARTBEAT_INTERVAL until told to stop.

    Runs concurrently with the job handler on its own DB session. `stop` is set
    the moment the handler finishes (success or failure), so we exit promptly
    rather than holding the lease past completion.
    """
    while not stop.is_set():
        try:
            # Wake either when the interval elapses or when the job finishes.
            await asyncio.wait_for(stop.wait(), timeout=HEARTBEAT_INTERVAL_SECONDS)
            return  # stop was signaled -> job done, nothing left to renew
        except asyncio.TimeoutError:
            pass  # interval elapsed -> time to renew
        try:
            async with get_session() as session:
                await extend_lease(session, job_id)
        except Exception as e:
            print(f"Heartbeat error for job {job_id}: {e}")


async def recover_expired_job(session) -> bool:
    """Reclaim a single job whose lease has lapsed; return True if one was found.

    Uses the same FOR UPDATE SKIP LOCKED pattern as claim_job, so every worker
    can run a reaper concurrently without two of them grabbing the same row. A
    dead worker is treated as just another failed attempt, so the job flows
    through the normal retry/backoff (or dead-letter) path in fail_job. The
    attempt was already counted when the job was first claimed, which also caps
    how many times a poison job that keeps killing workers can be retried.
    """
    result = await session.execute(
        select(Job)
        .where(Job.status == "running")
        .where(Job.lease_expires_at < datetime.now(timezone.utc))
        .limit(1)
        .with_for_update(skip_locked=True)
    )
    job = result.scalar_one_or_none()
    if job is None:
        return False
    print(f"Recovering job {job.id}: lease expired, worker presumed dead")
    await fail_job(session, job, "lease expired -- worker presumed dead")
    return True


async def run_reaper() -> None:
    """Background loop: periodically sweep for and recover lease-expired jobs."""
    print("Reaper started, watching for expired leases...")
    while True:
        try:
            async with get_session() as session:
                recovered = 0
                while await recover_expired_job(session):
                    recovered += 1
                if recovered:
                    print(f"Reaper recovered {recovered} expired job(s)")
        except Exception as e:
            print(f"Reaper error: {e}")
        await asyncio.sleep(REAPER_INTERVAL_SECONDS)


async def run_worker():
    print("Worker started, polling for jobs...")
    # Every worker also runs a reaper. Kept referenced so it isn't GC'd.
    reaper_task = asyncio.create_task(run_reaper())  # noqa: F841
    while True:
        async with get_session() as session:
            job = await claim_job(session)
            if job:
                print(f"Claimed job {job.id} of type {job.type}")
                # Keep the lease alive for as long as the handler runs.
                stop = asyncio.Event()
                heartbeat = asyncio.create_task(_heartbeat(job.id, stop))
                try:
                    handler = HANDLERS.get(job.type)
                    if not handler:
                        raise ValueError(f"No handler for job type: {job.type}")
                    result = await handler(job.payload)
                    await complete_job(session, job, result)
                    print(f"Completed job {job.id}")
                except Exception as e:
                    await fail_job(session, job, str(e))
                    print(f"Failed job {job.id}: {e}")
                finally:
                    stop.set()
                    await heartbeat
            else:
                await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(run_worker())
