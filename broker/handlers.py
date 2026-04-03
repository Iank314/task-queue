from aiohttp import web
from broker.database import get_session
from broker.models import Job, DeadLetterJob
from sqlalchemy import func, select
import uuid


async def submit_job(request: web.Request) -> web.Response:
    data = await request.json()
    async with get_session() as session:
        job = Job(
            type=data["type"],
            payload=data["payload"],
            priority=data.get("priority", 0),
        )
        session.add(job)
        await session.commit()
        await session.refresh(job)
    return web.json_response({"id": str(job.id)}, status=201)


async def get_job(request: web.Request) -> web.Response:
    job_id = request.match_info["id"]
    async with get_session() as session:
        job = await session.get(Job, uuid.UUID(job_id))
        if not job:
            raise web.HTTPNotFound()
    return web.json_response(
        {
            "id": str(job.id),
            "type": job.type,
            "status": job.status,
            "payload": job.payload,
            "priority": job.priority,
            "attempts": job.attempts,
            "max_attempts": job.max_attempts,
            "result": job.result,
            "error": job.error,
        }
    )


async def list_jobs(request: web.Request) -> web.Response:
    status_filter = request.query.get("status")
    async with get_session() as session:
        query = select(Job).order_by(Job.created_at.desc())
        if status_filter:
            query = query.where(Job.status == status_filter)
        result = await session.execute(query)
        jobs = result.scalars().all()
    return web.json_response(
        [
            {
                "id": str(j.id),
                "type": j.type,
                "status": j.status,
                "priority": j.priority,
                "attempts": j.attempts,
                "created_at": j.created_at.isoformat() if j.created_at else None,
            }
            for j in jobs
        ]
    )


async def cancel_job(request: web.Request) -> web.Response:
    job_id = request.match_info["id"]
    async with get_session() as session:
        job = await session.get(Job, uuid.UUID(job_id))
        if not job:
            raise web.HTTPNotFound()
        if job.status not in ("pending",):
            return web.json_response(
                {"error": f"Cannot cancel job with status '{job.status}'"},
                status=409,
            )
        job.status = "cancelled"
        await session.commit()
    return web.json_response({"id": str(job.id), "status": "cancelled"})


async def get_metrics(request: web.Request) -> web.Response:
    async with get_session() as session:
        rows = await session.execute(
            select(Job.status, func.count()).group_by(Job.status)
        )
        counts = {status: count for status, count in rows}
        dl_count = await session.execute(
            select(func.count()).select_from(DeadLetterJob)
        )
        counts["dead_letter"] = dl_count.scalar() or 0
    return web.json_response(counts)


async def list_dead_letter(request: web.Request) -> web.Response:
    async with get_session() as session:
        result = await session.execute(
            select(DeadLetterJob).order_by(DeadLetterJob.failed_at.desc())
        )
        jobs = result.scalars().all()
    return web.json_response(
        [
            {
                "id": str(j.id),
                "original_job_id": str(j.original_job_id),
                "type": j.type,
                "payload": j.payload,
                "priority": j.priority,
                "attempts": j.attempts,
                "error": j.error,
                "created_at": j.created_at.isoformat() if j.created_at else None,
                "failed_at": j.failed_at.isoformat() if j.failed_at else None,
            }
            for j in jobs
        ]
    )


async def retry_dead_letter(request: web.Request) -> web.Response:
    dl_id = request.match_info["id"]
    async with get_session() as session:
        dl_job = await session.get(DeadLetterJob, uuid.UUID(dl_id))
        if not dl_job:
            raise web.HTTPNotFound()
        new_job = Job(
            type=dl_job.type,
            payload=dl_job.payload,
            priority=dl_job.priority,
        )
        session.add(new_job)
        await session.delete(dl_job)
        await session.commit()
        await session.refresh(new_job)
    return web.json_response({"id": str(new_job.id), "status": "pending"}, status=201)


async def delete_dead_letter(request: web.Request) -> web.Response:
    dl_id = request.match_info["id"]
    async with get_session() as session:
        dl_job = await session.get(DeadLetterJob, uuid.UUID(dl_id))
        if not dl_job:
            raise web.HTTPNotFound()
        await session.delete(dl_job)
        await session.commit()
    return web.json_response({"status": "discarded"})
