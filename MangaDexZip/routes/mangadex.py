from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import RedirectResponse
from pydantic import BaseModel

from typing import Union

from ..queue import client

router = APIRouter(tags=["MangaDex"])


class NewTask(BaseModel):
    task_id: str


@router.get("/title/{manga_id}", summary="Download a Manga (user-friendly)")
@router.get("/manga/{manga_id}", include_in_schema=False)
@router.get("/title/{manga_id}/{garbage}", include_in_schema=False)
@router.get("/manga/{manga_id}/{garbage}", include_in_schema=False)
def add_manga(manga_id: str,
              request: Request,
              garbage: Union[str, None] = None,
              light: Union[str, None] = None,
              lang: Union[str, None] = "en") -> RedirectResponse:
    """Download a Manga.

    *front-end use only* - For API usage, please refer to the /api/manga endpoint.

    *This endpoint is named '/title' to match MangaDex's frontend paths. It is also aliased to '/manga'.*"""
    _ = garbage
    task = _add_manga(manga_id, request, light=light, lang=lang)

    api_host = f"{request.url.hostname}:{request.url.port}" if request.url.port else request.url.hostname
    api_url = f"{request.url.scheme}://{api_host}"
    return RedirectResponse(f"{api_url}/queue/front/{task['task_id']}/wait")


@router.get("/manga/{manga_id}", summary="Download a Manga (dev-friendly)")
def add_manga(manga_id: str,
              request: Request,
              light: Union[str, None] = None,
              lang: Union[str, None] = "en") -> NewTask:
    """Download a Manga.

    - `manga_id` must be a valid MangaDex Manga (Title).
    - `light` is optional and refers to the downsized version of chapter images.
    - `land` is optional, defaults to 'en' and refers to the language used when searching for chapters.

    *developer use only* - For regular usage, please refer to the /title endpoint."""
    task = _add_manga(manga_id, request, light=light, lang=lang)
    return NewTask(task_id=task["task_id"])


def _add_manga(manga_id: str,
               request: Request,
               light: Union[str, None] = None,
               lang: Union[str, None] = "en"):
    worker = client.select_worker_auto()
    if not worker:
        raise HTTPException(status_code=503, detail="No reachable workers, please try again later")

    opt = {
        "light": True if light in ("1", "true") else False,
        "language": lang or "en"
    }
    task = client.append(worker, "manga", manga_id, opt, request.client.host)
    if not task:
        raise HTTPException(status_code=502, detail="Couldn't reach worker, please try again later")
    return task


@router.get("/chapter/{chapter_id}", summary="Download a Chapter (user-friendly)")
@router.get("/chapter/{chapter_id}/{garbage}", include_in_schema=False)
def add_chapter(chapter_id: str,
                request: Request,
                garbage: Union[str, None] = None,
                light: Union[str, None] = None) -> RedirectResponse:
    """Download a Chapter.

    *front-end use only* - For API usage, please refer to the /api/chapter endpoint."""
    _ = garbage
    task = _add_chapter(chapter_id, request, light=light)

    api_host = f"{request.url.hostname}:{request.url.port}" if request.url.port else request.url.hostname
    api_url = f"{request.url.scheme}://{api_host}"
    return RedirectResponse(f"{api_url}/queue/front/{task['task_id']}/wait")


@router.get("/api/chapter/{chapter_id}", summary="Download a Chapter (dev-friendly)")
def add_chapter(chapter_id: str,
                request: Request,
                light: Union[str, None] = None) -> NewTask:
    """Start a new Download Chapter Task.

    - `chapter_id` must be a valid MangaDex Chapter.
    - `light` is optional and refers to the downsized version of chapter images.

    *developer use only* - For regular usage, please refer to the /chapter endpoint."""
    task = _add_chapter(chapter_id, request, light=light)
    return NewTask(task_id=task["task_id"])


def _add_chapter(chapter_id: str,
                 request: Request,
                 light: Union[str, None] = None):
    worker = client.select_worker_auto()
    if not worker:
        raise HTTPException(status_code=503, detail="No reachable workers, please try again later")

    opt = {
        "light": True if light in ("1", "true") else False
    }
    task = client.append(worker, "chapter", chapter_id, opt, request.client.host)
    if not task:
        raise HTTPException(status_code=502, detail="Couldn't reach worker, please try again later")
    return task
