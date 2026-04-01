from aiohttp import web
from broker.database import init_db
from broker.handlers import submit_job, get_job, list_jobs, cancel_job, get_metrics


async def on_startup(app):
    await init_db()


app = web.Application()
app.on_startup.append(on_startup)

app.router.add_post("/jobs", submit_job)
app.router.add_get("/jobs", list_jobs)
app.router.add_get("/jobs/{id}", get_job)
app.router.add_delete("/jobs/{id}", cancel_job)
app.router.add_get("/metrics", get_metrics)

if __name__ == "__main__":
    web.run_app(app, port=8000)
