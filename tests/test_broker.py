import pytest
import uuid
from aiohttp import web
from unittest.mock import patch, AsyncMock, MagicMock
from broker.handlers import submit_job, get_job, list_jobs, cancel_job, get_metrics


def create_app():
    """Create a fresh app instance for each test (avoids event loop reuse issues)."""
    application = web.Application()
    application.router.add_post("/jobs", submit_job)
    application.router.add_get("/jobs", list_jobs)
    application.router.add_get("/jobs/{id}", get_job)
    application.router.add_delete("/jobs/{id}", cancel_job)
    application.router.add_get("/metrics", get_metrics)
    return application


class TestSubmitJob:
    @pytest.mark.asyncio
    async def test_submit_job_returns_201(self, aiohttp_client):
        mock_job = MagicMock()
        mock_job.id = uuid.uuid4()

        mock_session = AsyncMock()
        mock_session.refresh = AsyncMock(return_value=None)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("broker.handlers.get_session", return_value=mock_session):
            with patch("broker.handlers.Job", return_value=mock_job):
                client = await aiohttp_client(create_app())
                resp = await client.post(
                    "/jobs",
                    json={"type": "send_email", "payload": {"to": "test@test.com"}},
                )
                assert resp.status == 201
                data = await resp.json()
                assert "id" in data

    @pytest.mark.asyncio
    async def test_submit_job_with_priority(self, aiohttp_client):
        mock_job = MagicMock()
        mock_job.id = uuid.uuid4()

        mock_session = AsyncMock()
        mock_session.refresh = AsyncMock(return_value=None)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("broker.handlers.get_session", return_value=mock_session):
            with patch("broker.handlers.Job", return_value=mock_job):
                client = await aiohttp_client(create_app())
                resp = await client.post(
                    "/jobs",
                    json={
                        "type": "process_data",
                        "payload": {"items": [1, 2, 3]},
                        "priority": 5,
                    },
                )
                assert resp.status == 201


class TestGetJob:
    @pytest.mark.asyncio
    async def test_get_job_returns_details(self, aiohttp_client):
        job_id = uuid.uuid4()
        mock_job = MagicMock()
        mock_job.id = job_id
        mock_job.type = "send_email"
        mock_job.status = "pending"
        mock_job.payload = {"to": "test@test.com"}
        mock_job.priority = 0
        mock_job.attempts = 0
        mock_job.max_attempts = 3
        mock_job.result = None
        mock_job.error = None

        mock_session = AsyncMock()
        mock_session.get = AsyncMock(return_value=mock_job)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("broker.handlers.get_session", return_value=mock_session):
            client = await aiohttp_client(create_app())
            resp = await client.get(f"/jobs/{job_id}")
            assert resp.status == 200
            data = await resp.json()
            assert data["status"] == "pending"
            assert data["type"] == "send_email"

    @pytest.mark.asyncio
    async def test_get_job_not_found(self, aiohttp_client):
        mock_session = AsyncMock()
        mock_session.get = AsyncMock(return_value=None)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("broker.handlers.get_session", return_value=mock_session):
            client = await aiohttp_client(create_app())
            resp = await client.get(f"/jobs/{uuid.uuid4()}")
            assert resp.status == 404


class TestCancelJob:
    @pytest.mark.asyncio
    async def test_cancel_pending_job(self, aiohttp_client):
        job_id = uuid.uuid4()
        mock_job = MagicMock()
        mock_job.id = job_id
        mock_job.status = "pending"

        mock_session = AsyncMock()
        mock_session.get = AsyncMock(return_value=mock_job)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("broker.handlers.get_session", return_value=mock_session):
            client = await aiohttp_client(create_app())
            resp = await client.delete(f"/jobs/{job_id}")
            assert resp.status == 200
            data = await resp.json()
            assert data["status"] == "cancelled"

    @pytest.mark.asyncio
    async def test_cancel_running_job_fails(self, aiohttp_client):
        job_id = uuid.uuid4()
        mock_job = MagicMock()
        mock_job.id = job_id
        mock_job.status = "running"

        mock_session = AsyncMock()
        mock_session.get = AsyncMock(return_value=mock_job)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("broker.handlers.get_session", return_value=mock_session):
            client = await aiohttp_client(create_app())
            resp = await client.delete(f"/jobs/{job_id}")
            assert resp.status == 409


class TestMetrics:
    @pytest.mark.asyncio
    async def test_get_metrics(self, aiohttp_client):
        mock_status_result = MagicMock()
        mock_status_result.__iter__ = MagicMock(
            return_value=iter([("pending", 5), ("completed", 10)])
        )

        mock_dl_result = MagicMock()
        mock_dl_result.scalar.return_value = 2

        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(
            side_effect=[mock_status_result, mock_dl_result]
        )
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("broker.handlers.get_session", return_value=mock_session):
            client = await aiohttp_client(create_app())
            resp = await client.get("/metrics")
            assert resp.status == 200
            data = await resp.json()
            assert data["pending"] == 5
            assert data["completed"] == 10
            assert data["dead_letter"] == 2
