from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from requests.exceptions import HTTPError

from typing import Union

from ..queue import client

router = APIRouter(tags=["Queue"])
templates = Jinja2Templates(directory="MangaDexZip/web")


class SystemInfo(BaseModel):
    workers: int
    busy_workers: int
    tasks: int
    active_tasks: int


class TaskSchedulerInfo(BaseModel):
    groups: int
    active_groups: int
    tasks: int
    active_tasks: int


class TaskGroupInfo(BaseModel):
    tasks: int
    active_tasks: int


class TaskInfo(BaseModel):
    uid: str
    started: bool
    completed: bool
    failed: bool
    status: Union[str, None]
    progress: int
    group: Union[TaskGroupInfo, None]
    scheduler: Union[TaskSchedulerInfo, None]
    redirect_uri: Union[str, None]


@router.get("/queue/front", summary="Get info on running tasks")
def queue_info() -> SystemInfo:
    """Get general info about running tasks."""
    _workers = 0
    _busy_workers = 0
    _tasks = 0
    _act_tasks = 0
    for k in client.BACKENDS.keys():
        q = client.query(k)
        if q:
            _workers += 1
            if q["active_tasks"]:
                _busy_workers += 1
            _tasks += q["tasks"]
            _act_tasks += q["active_tasks"]
    return SystemInfo(
        workers=_workers,
        busy_workers=_busy_workers,
        tasks=_tasks,
        active_tasks=_act_tasks
    )


@router.get("/queue/front/{task_id}", summary="Get info on a specific task")
def task_info(task_id: str,
              request: Request) -> TaskInfo:
    """Get info on a running task."""
    task = client.get_info(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    if task["completed"]:
        worker = client.BACKENDS[client.task_cache[task_id][0]]
        if worker["proxy_data"]:
            api_host = f"{request.url.hostname}:{request.url.port}" if request.url.port else request.url.hostname
            api_url = f"{request.url.scheme}://{api_host}"
            _r_uri = f"{api_url}/queue/front/{task_id}/data"
        else:
            _r_uri = f"{worker['url']}/queue/back/{task_id}/data"
    else:
        _r_uri = None

    _s_info = TaskSchedulerInfo(
        groups=task["scheduler"]["groups"],
        active_groups=task["scheduler"]["active_groups"],
        tasks=task["scheduler"]["tasks"],
        active_tasks=task["scheduler"]["active_tasks"]
    )

    _g_info = TaskGroupInfo(
        tasks=task["group"]["tasks"],
        active_tasks=task["group"]["active_tasks"]
    ) if task["group"] else None

    return TaskInfo(
        uid=task["uid"],
        started=task["started"],
        completed=task["completed"],
        failed=task["failed"],
        status=task["status"],
        progress=task["progress"],
        group=_g_info,
        scheduler=_s_info,
        redirect_uri=_r_uri
    )


# noinspection PyTypeChecker
@router.get("/queue/front/{task_id}/wait", summary="Wait for a running task")
def task_wait(task_id: str,
              request: Request) -> HTMLResponse:
    """*front-end use only* Wait for a running task.

    This page will send a small HTML structure and JS script that regularly refreshes the task's progress.
    The script will then redirect the user to the download link."""
    task = client.get_info(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    api_host = f"{request.url.hostname}:{request.url.port}" if request.url.port else request.url.hostname
    api_url = f"{request.url.scheme}://{api_host}"
    return templates.TemplateResponse("wait.html", {
        "request": request, "api_url": api_url,
        "task_id": task_id, "update_interval": "2500"
    })


@router.get("/queue/front/{task_id}/data", summary="Download a finished task's data")
def task_data(task_id: str):
    """Download data for a specific task.

    The data can only be retrieved for a completed task that hasn't failed.

    Note: This endpoint highly depends on the worker that processed the task.
    You should **always** follow the `redirect_uri` present in the task info endpoint.
    Not all tasks are retrievable from this endpoint.
    """
    task = client.get_info(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    try:
        data = client.proxy_data(task_id)
        return StreamingResponse(data.iter_content(chunk_size=8192), media_type="application/zip",
                                 headers={"Content-Disposition": data.headers.get("Content-Disposition"),
                                          "Content-Type": data.headers.get("Content-Type"),
                                          "Content-Length": data.headers.get("Content-Length"),
                                          "Last-Modified": data.headers.get("Last-Modified"),
                                          "ETag": data.headers.get("ETag")})
    except client.WorkerProxyDisabledError:
        raise HTTPException(status_code=400, detail="Task cannot be retrieved via this endpoint")
    except FileNotFoundError:
        raise HTTPException(status_code=500, detail="Cache out of date (this could only have happened if "
                                                    "the task expired at the moment of the retrieval)")
    except client.WorkerFileNotReadyError:
        raise HTTPException(status_code=403, detail="Task not ready")
    except HTTPError:
        raise HTTPException(status_code=500, detail="Error while retrieving file from worker")
