from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import RedirectResponse

from typing import Union

from ..queue import client

router = APIRouter(tags=["MangaDex"])


@router.get("/title/{manga_id}", summary="Download a Manga")
@router.get("/title/{manga_id}/{garbage}", summary="Download a Manga")
def add_manga(manga_id: str,
              garbage,
              request: Request,
              light: Union[str, None] = None,
              lang: Union[str, None] = "en") -> RedirectResponse:
    """*front-end use only* Download a Manga.

    For API usage, please refer to the /api/title endpoint."""
    _ = garbage
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

    api_host = f"{request.url.hostname}:{request.url.port}" if request.url.port else request.url.hostname
    api_url = f"{request.url.scheme}://{api_host}"
    return RedirectResponse(f"{api_url}/queue/front/{task['task_id']}/wait")


@router.get("/chapter/{chapter_id}", summary="Download a Chapter")
@router.get("/chapter/{chapter_id}/{garbage}", summary="Download a Chapter")
def add_chapter(chapter_id: str,
                garbage,
                request: Request,
                light: Union[str, None] = None) -> RedirectResponse:
    """*front-end use only* Download a Chapter.

    For API usage, please refer to the /api/manga endpoint."""
    _ = garbage
    worker = client.select_worker_auto()
    if not worker:
        raise HTTPException(status_code=503, detail="No reachable workers, please try again later")

    opt = {
        "light": True if light in ("1", "true") else False
    }
    task = client.append(worker, "chapter", chapter_id, opt, request.client.host)
    if not task:
        raise HTTPException(status_code=502, detail="Couldn't reach worker, please try again later")

    api_host = f"{request.url.hostname}:{request.url.port}" if request.url.port else request.url.hostname
    api_url = f"{request.url.scheme}://{api_host}"
    return RedirectResponse(f"{api_url}/queue/front/{task['task_id']}/wait")
