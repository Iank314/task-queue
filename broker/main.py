from aiohttp import web
from aiohttp_cors import setup as cors_setup, ResourceOptions
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
