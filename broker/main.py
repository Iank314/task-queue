import pathlib
from aiohttp import web
from aiohttp_cors import setup as cors_setup, ResourceOptions
from broker.database import init_db
from broker.handlers import (
    submit_job, get_job, list_jobs, cancel_job, get_metrics,
    list_dead_letter, retry_dead_letter, delete_dead_letter,
)

DASHBOARD_PATH = pathlib.Path(__file__).resolve().parent.parent / "dashboard" / "index.html"


async def on_startup(app):
    await init_db()


app = web.Application()
app.on_startup.append(on_startup)

async def dashboard(request):
    return web.FileResponse(DASHBOARD_PATH)

app.router.add_get("/dashboard", dashboard)
app.router.add_post("/jobs", submit_job)
app.router.add_get("/jobs", list_jobs)
app.router.add_get("/jobs/{id}", get_job)
app.router.add_delete("/jobs/{id}", cancel_job)
app.router.add_get("/metrics", get_metrics)
app.router.add_get("/dead-letter", list_dead_letter)
app.router.add_post("/dead-letter/{id}/retry", retry_dead_letter)
app.router.add_delete("/dead-letter/{id}", delete_dead_letter)

cors = cors_setup(app, defaults={
    "*": ResourceOptions(
        allow_credentials=True,
        expose_headers="*",
        allow_headers="*",
        allow_methods="*",
    )
})
for route in list(app.router.routes()):
    cors.add(route)

if __name__ == "__main__":
    web.run_app(app, port=8000)
