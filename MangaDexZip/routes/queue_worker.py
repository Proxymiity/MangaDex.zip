from fastapi import APIRouter, Header, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
from uuid import uuid4
from datetime import datetime
from json import dumps

from typing import Union, Annotated

from ..queue import manager, tasks, actions

from ..config import config


router = APIRouter(tags=["Queue Worker"], include_in_schema=not config["backend"]["hide_from_openapi"])
AUTH_TOKEN = config["backend"]["auth_token"]
ALWAYS_ALLOW_RETRIEVE = config["backend"]["always_allow_retrieve"]
ENFORCE_LIMITS = config["backend"]["enforce_limits"]


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


class BackendCompleteActionInfo(BaseModel):
    name: str
    data: dict
    unserializable_data: dict


class BackendCompleteTaskInfo(BackendTaskInfo):
    actions: list[Union[BackendCompleteActionInfo, dict]]
    queued_actions: list[Union[BackendCompleteActionInfo, dict]]
    status_real: Union[str, None]
    status_override: Union[str, None]


class BackendCompleteTaskGroupInfo(BackendTaskGroupInfo):
    tasks: dict[str, Union[BackendCompleteTaskInfo, dict]]
    active_tasks: dict[str, Union[BackendCompleteTaskInfo, dict]]
    queued_tasks: dict[str, Union[BackendCompleteTaskInfo, dict]]


class BackendCompleteTaskSchedulerInfo(BackendTaskSchedulerInfo):
    ready: bool
    groups: dict[str, Union[BackendCompleteTaskGroupInfo, dict]]
    active_groups: dict[str, Union[BackendCompleteTaskGroupInfo, dict]]
    queued_groups: dict[str, Union[BackendCompleteTaskGroupInfo, dict]]
    tasks: dict[str, Union[BackendCompleteTaskInfo, dict]]
    active_tasks: dict[str, Union[BackendCompleteTaskInfo, dict]]
    queued_tasks: dict[str, Union[BackendCompleteTaskInfo, dict]]
    actions: list[Union[BackendCompleteActionInfo, dict]]
    queued_actions: list[Union[BackendCompleteActionInfo, dict]]


def _is_json_serializable(o):
    try:
        dumps(o)
        return True
    except (TypeError, OverflowError):
        return False


@router.get("/queue/back", summary="Get general backend info",
            responses={
                403: {"description": "Invalid authorization token"}
            })
def queue_info(authorization: Annotated[Union[str, None], Header()] = None) -> BackendTaskSchedulerInfo:
    """Get general backend info about running tasks.

    This endpoint is used for internal communication between the queue_client and the queue_worker.
    If configured, this endpoint will require an authorization token."""
    if AUTH_TOKEN and authorization != AUTH_TOKEN:
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


@router.get("/queue/back/ready", summary="Get backend ready status",
            responses={
                403: {"description": "Invalid authorization token"}
            })
def queue_ready(authorization: Annotated[Union[str, None], Header()] = None) -> bool:
    """Get the ready status based on backend limits.

    This endpoint is used for internal communication between the queue_client and the queue_worker.
    If configured, this endpoint will require an authorization token."""
    if AUTH_TOKEN and authorization != AUTH_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid authorization token")
    return manager.check_status()


@router.get("/queue/back/all", summary="Get all existing tasks",
            responses={
                403: {"description": "Invalid authorization token"}
            })
def queue_info(authorization: Annotated[Union[str, None], Header()] = None) -> BackendCompleteTaskSchedulerInfo:
    """Get all registered tasks on this worker.

    Warning: This endpoint may impact performance when used.

    This endpoint is used for internal communication between the admin and the queue_worker.
    If configured, this endpoint will require an authorization token."""
    if AUTH_TOKEN and authorization != AUTH_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid authorization token")
    _g, _ag, _qg = {}, {}, {}
    _t, _at, _qt = {}, {}, {}
    _a, _qa = [], []
    scheduler = manager.scheduler
    for g in scheduler.groups:
        g_tasks = {}
        g_active_tasks = {}
        g_queued_tasks = {}

        for t in g.tasks:
            t_actions = []
            t_queued_actions = []

            for a in t.actions:
                a_info_serializable = [k for k, v in a.__dict__.items() if _is_json_serializable(v)]
                a_info = BackendCompleteActionInfo(
                    name=a.__class__.__name__,
                    data={k: v for k, v in a.__dict__.items() if k in a_info_serializable},
                    unserializable_data={k: str(v) for k, v in a.__dict__.items() if k not in a_info_serializable}
                )

                t_actions.append(a_info)
                _a.append(a_info)
                if a in t.queued_actions:
                    t_queued_actions.append(a_info)
                    _qa.append(a_info)

            t_info = BackendCompleteTaskInfo(
                uid=t.uid,
                kind=t.kind,
                actions=t_actions,
                queued_actions=t_queued_actions,
                started=t.started,
                completed=t.completed,
                failed=t.failed,
                status=t.status_override or t.status,
                status_real=t.status,
                status_override=t.status_override,
                result=t.result,
                progress=t.progress,
                created_at=t.created_at,
                group=None,
                scheduler=None
            )

            g_tasks[t.uid] = t_info
            _t[t.uid] = t_info
            if t in g.active_tasks:
                g_active_tasks[t.uid] = t_info
                _at[t.uid] = t_info
            if t in g.queued_tasks:
                g_queued_tasks[t.uid] = t_info
                _qt[t.uid] = t_info

        g_info = BackendCompleteTaskGroupInfo(
            uid=g.uid,
            tasks=g_tasks,
            queued_tasks=g_queued_tasks,
            active_tasks=g_active_tasks
        )

        _g[g.uid] = g_info
        if g in scheduler.active_groups:
            _ag[g.uid] = g_info
        if g in scheduler.queued_groups:
            _qg[g.uid] = g_info

    return BackendCompleteTaskSchedulerInfo(
        ready=manager.check_status(),
        groups=_g,
        active_groups=_ag,
        queued_groups=_qg,
        tasks=_t,
        active_tasks=_at,
        queued_tasks=_qt,
        actions=_a,
        queued_actions=_qa
    )


@router.post("/queue/back/new", summary="Add a new task to the queue",
             responses={
                 400: {"description": "Invalid task type"},
                 403: {"description": "Invalid authorization token"},
                 503: {"description": "Worker not ready"}
             })
def queue_append(new_task: BackendTaskRequest,
                 authorization: Annotated[Union[str, None], Header()] = None) -> BackendTaskResponse:
    """Add a new task to the queue.

    `type` should be one of the following: manga, chapter.

    `data` should be the base action data, and `opt_data` corresponds to extended task data.

    `group` specifies the TaskGroup's uid used for queue fairness.

    This endpoint is used for internal communication between the queue_client and the queue_worker.
    If configured, this endpoint will require an authorization token."""
    if AUTH_TOKEN and authorization != AUTH_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid authorization token")
    if ENFORCE_LIMITS and not manager.check_status():
        raise HTTPException(status_code=503, detail="Worker status degraded")
    if new_task.type == "manga":
        task = tasks.Task.get_task(str(uuid4()))
        task.kind = "download_archive"
        task.add_action(actions.AddMangaChapters(new_task.data,
                                                 light=new_task.opt_data.get("light", False),
                                                 language=new_task.opt_data.get("language", "en"),
                                                 append_titles=new_task.opt_data.get("append_titles", False),
                                                 preferred_groups=new_task.opt_data.get("preferred_groups", []),
                                                 groups_substitute=new_task.opt_data.get("groups_substitute", True),
                                                 start=new_task.opt_data.get("start", None),
                                                 end=new_task.opt_data.get("end", None)))
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


@router.get("/queue/back/{task_id}", summary="Get info for a specific task",
            responses={
                403: {"description": "Invalid authorization token"},
                404: {"description": "Task not found"}
            })
def task_info(task_id: str,
              authorization: Annotated[Union[str, None], Header()] = None) -> BackendTaskInfo:
    """Get info for a specific task in the queue.

    This endpoint is used for internal communication between the queue_client and the queue_worker.
    If configured, this endpoint will require an authorization token."""
    if AUTH_TOKEN and authorization != AUTH_TOKEN:
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
        status=task.status_override or task.status,
        result=task.result,
        progress=task.progress,
        created_at=task.created_at,
        group=_g_info,
        scheduler=_s_info
    )


@router.delete("/queue/back/{task_id}", summary="Cancel a running task",
               responses={
                   403: {"description": "Invalid authorization token"},
                   404: {"description": "Task not found"}
               })
def task_info(task_id: str,
              authorization: Annotated[Union[str, None], Header()] = None):
    """Cancel a running task in the queue.

    This endpoint is used for internal communication between the queue_client and the queue_worker.
    If configured, this endpoint will require an authorization token."""
    if AUTH_TOKEN and authorization != AUTH_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid authorization token")
    if task_id not in tasks.Task.instances:
        raise HTTPException(status_code=404, detail="Task not found")

    task = tasks.Task.get_task(uid=task_id)
    task.failed = True
    task.status_override = "Task execution cancelled"

    return True


@router.get("/queue/back/{task_id}/data", summary="Retrieve data for a specific task",
            responses={
                403: {"description": "Invalid authorization token, task not ready"},
                404: {"description": "Task not found"},
                503: {"description": "Task not supported"}
            },
            response_class=FileResponse)
async def task_data(task_id: str,
                    authorization: Annotated[Union[str, None], Header()] = None) -> FileResponse:
    """Retrieve data for a specific task

    The data can only be retrieved for a completed task that hasn't failed.

    This endpoint is used for internal communication between the queue_client and the queue_worker.
    If configured, this endpoint will require an authorization token."""
    if not ALWAYS_ALLOW_RETRIEVE and AUTH_TOKEN and authorization != AUTH_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid authorization token")
    if task_id not in tasks.Task.instances:
        raise HTTPException(status_code=404, detail="Task not found")
    task = tasks.Task.get_task(uid=task_id)
    if not task.completed or not task.result or task.failed:
        raise HTTPException(status_code=403, detail="Cannot retrieve data from an unfinished task")
    if task.kind == "download_archive":
        return FileResponse(task.result, filename=f"{task.uid}.zip", media_type="application/zip")
    else:
        raise HTTPException(status_code=503, detail="Unknown task kind")
