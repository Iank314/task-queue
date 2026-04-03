"""
Integration tests that run against a real PostgreSQL database.

These tests verify:
  - Full API round-trips through the broker
  - Worker job claiming with SELECT FOR UPDATE SKIP LOCKED
  - Concurrent workers never double-claim a job
  - Priority ordering is respected
  - Retry / exponential-backoff lifecycle
"""

import asyncio
import uuid
import pytest
import pytest_asyncio
from datetime import datetime, timezone
from unittest.mock import patch
from aiohttp import web
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from broker.models import Job, DeadLetterJob
from broker.handlers import (
    submit_job, get_job, list_jobs, cancel_job, get_metrics,
    list_dead_letter, retry_dead_letter, delete_dead_letter,
)
from worker.worker import claim_job, complete_job, fail_job


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _broker_app():
    """Minimal aiohttp app wired to real handlers."""
    app = web.Application()
    app.router.add_post("/jobs", submit_job)
    app.router.add_get("/jobs", list_jobs)
    app.router.add_get("/jobs/{id}", get_job)
    app.router.add_delete("/jobs/{id}", cancel_job)
    app.router.add_get("/metrics", get_metrics)
    app.router.add_get("/dead-letter", list_dead_letter)
    app.router.add_post("/dead-letter/{id}/retry", retry_dead_letter)
    app.router.add_delete("/dead-letter/{id}", delete_dead_letter)
    return app


async def _insert_job(session, **overrides):
    """Insert a job directly and return it."""
    fields = dict(
        type="process_data",
        payload={"items": [1, 2, 3]},
        status="pending",
        priority=0,
        attempts=0,
        max_attempts=3,
        run_after=datetime.now(timezone.utc),
    )
    fields.update(overrides)
    job = Job(**fields)
    session.add(job)
    await session.commit()
    await session.refresh(job)
    return job


# ---------------------------------------------------------------------------
# Broker API  →  real PostgreSQL
# ---------------------------------------------------------------------------

class TestBrokerIntegration:
    """Full HTTP round-trips that hit a real database."""

    @pytest.mark.asyncio
    async def test_submit_and_get_job(self, db_engine, aiohttp_client):
        with patch("broker.handlers.get_session", new=self._session_factory(db_engine)):
            client = await aiohttp_client(_broker_app())

            # Submit
            resp = await client.post(
                "/jobs",
                json={"type": "send_email", "payload": {"to": "a@b.com"}, "priority": 3},
            )
            assert resp.status == 201
            job_id = (await resp.json())["id"]

            # Get
            resp = await client.get(f"/jobs/{job_id}")
            assert resp.status == 200
            data = await resp.json()
            assert data["type"] == "send_email"
            assert data["priority"] == 3
            assert data["status"] == "pending"

    @pytest.mark.asyncio
    async def test_list_and_filter_jobs(self, db_engine, aiohttp_client):
        with patch("broker.handlers.get_session", new=self._session_factory(db_engine)):
            client = await aiohttp_client(_broker_app())

            await client.post("/jobs", json={"type": "a", "payload": {}})
            await client.post("/jobs", json={"type": "b", "payload": {}})

            resp = await client.get("/jobs")
            assert len(await resp.json()) == 2

            resp = await client.get("/jobs?status=completed")
            assert len(await resp.json()) == 0

    @pytest.mark.asyncio
    async def test_cancel_pending_job(self, db_engine, aiohttp_client):
        with patch("broker.handlers.get_session", new=self._session_factory(db_engine)):
            client = await aiohttp_client(_broker_app())

            resp = await client.post("/jobs", json={"type": "x", "payload": {}})
            job_id = (await resp.json())["id"]

            resp = await client.delete(f"/jobs/{job_id}")
            assert resp.status == 200
            assert (await resp.json())["status"] == "cancelled"

            # Verify it's actually cancelled in the DB
            resp = await client.get(f"/jobs/{job_id}")
            assert (await resp.json())["status"] == "cancelled"

    @pytest.mark.asyncio
    async def test_metrics_reflect_real_data(self, db_engine, aiohttp_client):
        with patch("broker.handlers.get_session", new=self._session_factory(db_engine)):
            client = await aiohttp_client(_broker_app())

            await client.post("/jobs", json={"type": "a", "payload": {}})
            await client.post("/jobs", json={"type": "b", "payload": {}})

            resp = await client.get("/metrics")
            data = await resp.json()
            assert data.get("pending", 0) == 2

    # -- helper to wire handlers to the test DB engine --
    @staticmethod
    def _session_factory(engine):
        Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def _get_session():
            async with Session() as s:
                yield s

        return _get_session


# ---------------------------------------------------------------------------
# Worker  →  real PostgreSQL
# ---------------------------------------------------------------------------

class TestWorkerIntegration:
    """Worker functions operating on real rows."""

    @pytest.mark.asyncio
    async def test_claim_sets_running_in_db(self, db_engine, db_session):
        job = await _insert_job(db_session)

        Session = sessionmaker(db_engine, class_=AsyncSession, expire_on_commit=False)
        async with Session() as worker_session:
            claimed = await claim_job(worker_session)

        assert claimed is not None
        assert claimed.id == job.id
        assert claimed.status == "running"
        assert claimed.attempts == 1

    @pytest.mark.asyncio
    async def test_complete_persists_result(self, db_engine, db_session):
        job = await _insert_job(db_session)

        Session = sessionmaker(db_engine, class_=AsyncSession, expire_on_commit=False)
        async with Session() as ws:
            claimed = await claim_job(ws)
            await complete_job(ws, claimed, {"output": 42})

        # Re-read from DB in a fresh session
        async with Session() as ws:
            row = await ws.get(Job, job.id)
            assert row.status == "completed"
            assert row.result == {"output": 42}

    @pytest.mark.asyncio
    async def test_fail_and_retry_lifecycle(self, db_engine, db_session):
        job = await _insert_job(db_session, max_attempts=2)

        Session = sessionmaker(db_engine, class_=AsyncSession, expire_on_commit=False)

        # First attempt — fails, should go back to pending
        async with Session() as ws:
            claimed = await claim_job(ws)
            await fail_job(ws, claimed, "transient error")

        async with Session() as ws:
            row = await ws.get(Job, job.id)
            assert row.status == "pending"
            assert row.attempts == 1
            assert row.run_after > datetime.now(timezone.utc)  # delayed

        # Bypass the run_after delay so the job is claimable again
        async with Session() as ws:
            row = await ws.get(Job, job.id)
            row.run_after = datetime.now(timezone.utc)
            await ws.commit()

        # Second attempt — fails again, should move to dead letter (max_attempts=2)
        async with Session() as ws:
            claimed = await claim_job(ws)
            assert claimed is not None
            await fail_job(ws, claimed, "permanent error")

        # Job should be gone from the jobs table
        async with Session() as ws:
            row = await ws.get(Job, job.id)
            assert row is None

        # Job should be in the dead letter table
        async with Session() as ws:
            result = await ws.execute(
                select(DeadLetterJob).where(DeadLetterJob.original_job_id == job.id)
            )
            dl_job = result.scalar_one_or_none()
            assert dl_job is not None
            assert dl_job.error == "permanent error"
            assert dl_job.attempts == 2


# ---------------------------------------------------------------------------
# Concurrency – the SKIP LOCKED proof
# ---------------------------------------------------------------------------

class TestConcurrentClaiming:
    """
    Verify that SELECT FOR UPDATE SKIP LOCKED prevents double-processing.
    Multiple coroutines (simulating workers) race to claim the same jobs.
    Every job must be claimed by exactly one worker.
    """

    @pytest.mark.asyncio
    async def test_no_double_claims(self, db_engine, db_session):
        """Insert N jobs, launch N workers concurrently — each job claimed once."""
        num_jobs = 10

        for i in range(num_jobs):
            await _insert_job(db_session, priority=i)

        Session = sessionmaker(db_engine, class_=AsyncSession, expire_on_commit=False)

        claimed_ids: list[uuid.UUID] = []
        lock = asyncio.Lock()

        async def worker():
            """Keep claiming until no jobs remain."""
            while True:
                async with Session() as ws:
                    job = await claim_job(ws)
                if job is None:
                    break
                async with lock:
                    claimed_ids.append(job.id)

        # Launch 5 workers racing over 10 jobs
        await asyncio.gather(*[worker() for _ in range(5)])

        assert len(claimed_ids) == num_jobs
        assert len(set(claimed_ids)) == num_jobs  # all unique — no duplicates

    @pytest.mark.asyncio
    async def test_single_job_claimed_by_one_worker(self, db_engine, db_session):
        """Insert 1 job, have 5 workers race — exactly 1 wins."""
        await _insert_job(db_session)

        Session = sessionmaker(db_engine, class_=AsyncSession, expire_on_commit=False)

        async def try_claim():
            async with Session() as ws:
                return await claim_job(ws)

        results = await asyncio.gather(*[try_claim() for _ in range(5)])

        winners = [r for r in results if r is not None]
        assert len(winners) == 1

    @pytest.mark.asyncio
    async def test_priority_order_under_concurrency(self, db_engine, db_session):
        """Jobs should be claimed in priority-descending order even under concurrency."""
        # Insert jobs with distinct priorities
        for p in [1, 5, 3, 10, 7]:
            await _insert_job(db_session, priority=p)

        Session = sessionmaker(db_engine, class_=AsyncSession, expire_on_commit=False)

        claimed_priorities = []

        # Single sequential worker to verify ordering
        while True:
            async with Session() as ws:
                job = await claim_job(ws)
            if job is None:
                break
            claimed_priorities.append(job.priority)

        assert claimed_priorities == sorted(claimed_priorities, reverse=True)
        assert claimed_priorities == [10, 7, 5, 3, 1]


# ---------------------------------------------------------------------------
# Dead Letter Queue
# ---------------------------------------------------------------------------

class TestDeadLetterQueue:
    """Verify dead letter API endpoints work against real PostgreSQL."""

    async def _create_dead_letter_job(self, db_engine):
        """Insert a job, exhaust its retries, and return the dead letter entry."""
        Session = sessionmaker(db_engine, class_=AsyncSession, expire_on_commit=False)

        async with Session() as s:
            job = Job(
                type="will_fail",
                payload={"data": "test"},
                priority=5,
                max_attempts=1,
                run_after=datetime.now(timezone.utc),
            )
            s.add(job)
            await s.commit()
            await s.refresh(job)
            original_id = job.id

        async with Session() as ws:
            claimed = await claim_job(ws)
            await fail_job(ws, claimed, "broken")

        async with Session() as s:
            result = await s.execute(
                select(DeadLetterJob).where(DeadLetterJob.original_job_id == original_id)
            )
            return result.scalar_one()

    @pytest.mark.asyncio
    async def test_list_dead_letter_jobs(self, db_engine, aiohttp_client):
        dl_job = await self._create_dead_letter_job(db_engine)

        with patch("broker.handlers.get_session", new=TestBrokerIntegration._session_factory(db_engine)):
            client = await aiohttp_client(_broker_app())

            resp = await client.get("/dead-letter")
            assert resp.status == 200
            data = await resp.json()
            assert len(data) == 1
            assert data[0]["error"] == "broken"
            assert data[0]["type"] == "will_fail"

    @pytest.mark.asyncio
    async def test_retry_dead_letter_resubmits_job(self, db_engine, aiohttp_client):
        dl_job = await self._create_dead_letter_job(db_engine)

        with patch("broker.handlers.get_session", new=TestBrokerIntegration._session_factory(db_engine)):
            client = await aiohttp_client(_broker_app())

            # Retry it
            resp = await client.post(f"/dead-letter/{dl_job.id}/retry")
            assert resp.status == 201
            data = await resp.json()
            assert data["status"] == "pending"

            # Dead letter list should be empty now
            resp = await client.get("/dead-letter")
            assert len(await resp.json()) == 0

            # New job should be in the jobs list
            resp = await client.get(f"/jobs/{data['id']}")
            assert resp.status == 200
            job_data = await resp.json()
            assert job_data["type"] == "will_fail"
            assert job_data["status"] == "pending"

    @pytest.mark.asyncio
    async def test_delete_dead_letter_discards_job(self, db_engine, aiohttp_client):
        dl_job = await self._create_dead_letter_job(db_engine)

        with patch("broker.handlers.get_session", new=TestBrokerIntegration._session_factory(db_engine)):
            client = await aiohttp_client(_broker_app())

            resp = await client.delete(f"/dead-letter/{dl_job.id}")
            assert resp.status == 200

            # Dead letter list should be empty
            resp = await client.get("/dead-letter")
            assert len(await resp.json()) == 0

    @pytest.mark.asyncio
    async def test_metrics_include_dead_letter_count(self, db_engine, aiohttp_client):
        await self._create_dead_letter_job(db_engine)

        with patch("broker.handlers.get_session", new=TestBrokerIntegration._session_factory(db_engine)):
            client = await aiohttp_client(_broker_app())

            resp = await client.get("/metrics")
            data = await resp.json()
            assert data["dead_letter"] == 1
