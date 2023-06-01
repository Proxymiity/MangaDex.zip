from fastapi import APIRouter, Header, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
from uuid import uuid4
from datetime import datetime

from typing import Union, Annotated

from ..queue import manager, tasks, actions

from ..config import config


router = APIRouter(tags=["Queue Worker"])
AUTH_TOKEN = config["backend"]["auth_token"]
ALWAYS_ALLOW_RETRIEVE = config["backend"]["always_allow_retrieve"]


class BackendTaskSchedulerInfo(BaseModel):
    groups: int
    active_groups: int
    queued_groups: int
    tasks: int
    active_tasks: int
    queued_tasks: int
    actions: int
    queued_actions: int


class BackendTaskGroupInfo(BaseModel):
    uid: str
    tasks: int
    active_tasks: int
    queued_tasks: int


class BackendTaskRequest(BaseModel):
    type: str
    data: str
    opt_data: Union[dict, None] = {}
    group: str


class BackendTaskResponse(BaseModel):
    task_id: str


class BackendTaskInfo(BaseModel):
    uid: str
    kind: Union[str, None]
    actions: int
    queued_actions: int
    started: bool
    completed: bool
    failed: bool
    status: Union[str, None]
    result: Union[str, None]
    progress: int
    created_at: datetime
    group: Union[BackendTaskGroupInfo, None]
    scheduler: Union[BackendTaskSchedulerInfo, None]


@router.get("/queue/back", summary="Get general backend info")
def queue_info(authorization: Annotated[Union[str, None], Header()] = None) -> BackendTaskSchedulerInfo:
    """Get general backend info about running tasks.

    This endpoint is used for internal communication between the queue_client and the queue_worker.
    If configured, this endpoint will require an authorization token."""
    if authorization != AUTH_TOKEN and AUTH_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid authorization token")
    scheduler = manager.scheduler
    return BackendTaskSchedulerInfo(
        groups=len(scheduler.groups),
        active_groups=len(scheduler.active_groups),
        queued_groups=len(scheduler.queued_groups),
        tasks=sum([len(g.tasks) for g in scheduler.groups]),
        active_tasks=sum([len(g.active_tasks) for g in scheduler.active_groups]),
        queued_tasks=sum([len(g.queued_tasks) for g in scheduler.active_groups]),
        actions=sum([sum([len(t.actions) for t in g.tasks]) for g in scheduler.groups]),
        queued_actions=sum([sum([len(t.queued_actions) for t in g.active_tasks]) for g in scheduler.active_groups])
    )


@router.post("/queue/back/new", summary="Add a new task to the queue")
def queue_append(new_task: BackendTaskRequest,
                 authorization: Annotated[Union[str, None], Header()] = None) -> BackendTaskResponse:
    """Add a new task to the queue.

    `type` should be one of the following: manga, chapter.

    `data` should be the base action data, and `opt_data` corresponds to extended task data.

    `group` specifies the TaskGroup's uid used for queue fairness.

    This endpoint is used for internal communication between the queue_client and the queue_worker.
    If configured, this endpoint will require an authorization token."""
    if authorization != AUTH_TOKEN and AUTH_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid authorization token")
    if new_task.type == "manga":
        task = tasks.Task.get_task(str(uuid4()))
        task.kind = "download_archive"
        task.add_action(actions.AddMangaChapters(new_task.data,
                                                 light=new_task.opt_data.get("light", False),
                                                 language=new_task.opt_data.get("language", "en")))
        group = tasks.TaskGroup.get_group(new_task.group)
        group.add_task(task)
        manager.scheduler.add_group(group)
        return BackendTaskResponse(task_id=task.uid)
    elif new_task.type == "chapter":
        task = tasks.Task.get_task(str(uuid4()))
        task.kind = "download_archive"
        task.add_action(actions.DownloadChapter(new_task.data,
                                                light=new_task.opt_data.get("light", False)))
        task.add_action(actions.ArchiveContentsZIP())
        group = tasks.TaskGroup.get_group(new_task.group)
        group.add_task(task)
        manager.scheduler.add_group(group)
        return BackendTaskResponse(task_id=task.uid)
    else:
        raise HTTPException(status_code=400, detail="Unsupported task type")


@router.get("/queue/back/{task_id}", summary="Get info for a specific task")
def task_info(task_id: str,
              authorization: Annotated[Union[str, None], Header()] = None) -> BackendTaskInfo:
    """Get info for a specific task in the queue.

    If the task doesn't exist, an empty task will be returned.

    This endpoint is used for internal communication between the queue_client and the queue_worker.
    If configured, this endpoint will require an authorization token."""
    if authorization != AUTH_TOKEN and AUTH_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid authorization token")
    if task_id not in tasks.Task.instances:
        raise HTTPException(status_code=404, detail="Task not found")

    scheduler = manager.scheduler
    _s_info = BackendTaskSchedulerInfo(
        groups=len(scheduler.groups),
        active_groups=len(scheduler.active_groups),
        queued_groups=len(scheduler.queued_groups),
        tasks=sum([len(g.tasks) for g in scheduler.groups]),
        active_tasks=sum([len(g.active_tasks) for g in scheduler.active_groups]),
        queued_tasks=sum([len(g.queued_tasks) for g in scheduler.active_groups]),
        actions=sum([sum([len(t.actions) for t in g.tasks]) for g in scheduler.groups]),
        queued_actions=sum([sum([len(t.queued_actions) for t in g.active_tasks]) for g in scheduler.active_groups])
    )

    task = tasks.Task.get_task(uid=task_id)
    _g_info = BackendTaskGroupInfo(
        uid=task.parent.uid,
        tasks=len(task.parent.tasks),
        active_tasks=len(task.parent.active_tasks),
        queued_tasks=len(task.parent.queued_tasks)
    ) if task.parent else None

    return BackendTaskInfo(
        uid=task.uid,
        kind=task.kind,
        actions=len(task.actions),
        queued_actions=len(task.queued_actions),
        started=task.started,
        completed=task.completed,
        failed=task.failed,
        status=task.status,
        result=task.result,
        progress=task.progress,
        created_at=task.created_at,
        group=_g_info,
        scheduler=_s_info
    )


@router.get("/queue/back/{task_id}/data", summary="Retrieve data for a specific task")
async def task_data(task_id: str,
                    authorization: Annotated[Union[str, None], Header()] = None) -> FileResponse:
    """Retrieve data for a specific task

    The data can only be retrieved for a completed task that hasn't failed.

    This endpoint is used for internal communication between the queue_client and the queue_worker.
    If configured, this endpoint will require an authorization token."""
    if authorization != AUTH_TOKEN and AUTH_TOKEN and not ALWAYS_ALLOW_RETRIEVE:
        raise HTTPException(status_code=403, detail="Invalid authorization token")
    if task_id not in tasks.Task.instances:
        raise HTTPException(status_code=404, detail="Task not found")
    task = tasks.Task.get_task(uid=task_id)
    if not task.completed or not task.result or task.failed:
        raise HTTPException(status_code=403, detail="Cannot retrieve data from an unfinished task")
    if task.kind == "download_archive":
        return FileResponse(task.result, filename=f"{task.uid}.zip", media_type="application/zip")
    else:
        raise HTTPException(status_code=500, detail="Unknown task kind")
