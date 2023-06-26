from fastapi import FastAPI
from fastapi.responses import HTMLResponse, PlainTextResponse

from pathlib import Path

from .routes import mangadex, queue_client, queue_worker, admin

from .config import config
from .stats import stats
from .queue import manager

__version__ = "1.0.1"

tags_metadata = []

if config["frontend"]["enabled"]:
    tags_metadata.append({
        "name": "MangaDex",
        "description": "Allows starting a new download job"
    })
    tags_metadata.append({
            "name": "Queue",
            "description": "Allows to retrieve information from a present or past download job"
    })
if config["backend"]["enabled"] and not config["backend"]["hide_from_openapi"]:
    tags_metadata.append({
        "name": "Queue Worker",
        "description": "Allows starting raw download jobs "
                       "(this endpoint is token-protected should not be used by end-users)"
    })
if config["admin"]["enabled"] and not config["admin"]["hide_from_openapi"]:
    tags_metadata.append({
        "name": "Admin",
        "description": "Allows administrating the instance "
                       "(this endpoint is token-protected should not be used by end-users)"
    })

app = FastAPI(
    title="MangaDex Zipper",
    description="The MangaDex Zipper allows batched downloading of Manga and Chapters from MangaDex.",
    version=__version__,
    openapi_tags=tags_metadata
)

if config["frontend"]["enabled"] is True:
    app.include_router(mangadex.router)
    app.include_router(queue_client.router)
if config["backend"]["enabled"] is True:
    app.include_router(queue_worker.router)
    manager.scheduler_thread.start()
    manager.cleanup_thread.start()
if config["admin"]["enabled"] is True:
    app.include_router(admin.router)


@app.on_event("shutdown")
def shutdown():
    if config["frontend"]["enabled"] is True:
        stats.save()


@app.get("/", summary="Index", include_in_schema=False)
def index() -> HTMLResponse:
    return HTMLResponse(Path("MangaDexZip/web/index.html").read_bytes())


@app.get("/robots.txt", include_in_schema=False)
def robots() -> PlainTextResponse:
    data = ["User-agent: *", "Disallow: /", "Allow: /$"]
    return PlainTextResponse("\n".join(data))


@app.get("/ping", summary="Ping",
         responses={
             200: {"content": {"application/json": {"example": {"message": "pong", "version": "x.y.z"}}}},
         })
def ping():
    return {"message": "pong", "version": __version__}
