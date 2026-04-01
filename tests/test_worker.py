import pytest
import uuid
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone, timedelta
from worker.worker import claim_job, complete_job, fail_job


@pytest.fixture
def mock_job():
    job = MagicMock()
    job.id = uuid.uuid4()
    job.type = "send_email"
    job.status = "pending"
    job.payload = {"to": "test@test.com"}
    job.priority = 0
    job.attempts = 0
    job.max_attempts = 3
    job.result = None
    job.error = None
    job.run_after = datetime.now(timezone.utc) - timedelta(seconds=10)
    job.updated_at = datetime.now(timezone.utc)
    return job


class TestClaimJob:
    @pytest.mark.asyncio
    async def test_claim_job_sets_running(self, mock_job):
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_job

        session = AsyncMock()
        session.execute = AsyncMock(return_value=mock_result)

        job = await claim_job(session)

        assert job is not None
        assert job.status == "running"
        assert job.attempts == 1
        session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_claim_job_returns_none_when_empty(self):
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None

        session = AsyncMock()
        session.execute = AsyncMock(return_value=mock_result)

        job = await claim_job(session)
        assert job is None
        session.commit.assert_not_awaited()


class TestCompleteJob:
    @pytest.mark.asyncio
    async def test_complete_job_sets_status(self, mock_job):
        session = AsyncMock()
        result = {"sent_to": "test@test.com"}

        await complete_job(session, mock_job, result)

        assert mock_job.status == "completed"
        assert mock_job.result == result
        session.commit.assert_awaited_once()


class TestFailJob:
    @pytest.mark.asyncio
    async def test_fail_job_retries_when_attempts_remain(self, mock_job):
        mock_job.attempts = 1
        mock_job.max_attempts = 3

        session = AsyncMock()
        await fail_job(session, mock_job, "some error")

        assert mock_job.status == "pending"
        assert mock_job.error == "some error"
        assert mock_job.run_after > datetime.now(timezone.utc)
        session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_fail_job_marks_failed_when_max_attempts(self, mock_job):
        mock_job.attempts = 3
        mock_job.max_attempts = 3

        session = AsyncMock()
        await fail_job(session, mock_job, "final error")

        assert mock_job.status == "failed"
        assert mock_job.error == "final error"
        session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_fail_job_exponential_backoff(self, mock_job):
        mock_job.attempts = 2
        mock_job.max_attempts = 3

        session = AsyncMock()
        before = datetime.now(timezone.utc)
        await fail_job(session, mock_job, "retry error")

        # delay should be 2^2 = 4 seconds
        expected_min = before + timedelta(seconds=3)
        assert mock_job.run_after > expected_min
