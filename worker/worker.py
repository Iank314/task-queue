import asyncio
from datetime import datetime, timezone, timedelta
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from contextlib import asynccontextmanager
from broker.models import Base, Job
from worker.handlers import HANDLERS
import os

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://user:password@postgres/taskqueue")

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
        job.status = "running"
        job.attempts += 1
        job.updated_at = datetime.now(timezone.utc)
        await session.commit()
    return job


async def complete_job(session, job: Job, result: dict):
    job.status = "completed"
    job.result = result
    job.updated_at = datetime.now(timezone.utc)
    await session.commit()


async def fail_job(session, job: Job, error: str):
    job.error = error
    if job.attempts >= job.max_attempts:
        job.status = "failed"
    else:
        job.status = "pending"
        delay = 2 ** job.attempts  # 2s, 4s, 8s
        job.run_after = datetime.now(timezone.utc) + timedelta(seconds=delay)
    job.updated_at = datetime.now(timezone.utc)
    await session.commit()


async def run_worker():
    print("Worker started, polling for jobs...")
    while True:
        async with get_session() as session:
            job = await claim_job(session)
            if job:
                print(f"Claimed job {job.id} of type {job.type}")
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
            else:
                await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(run_worker())
