from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel

from typing import Union, Annotated

from ..queue import client
from .queue_worker import BackendCompleteTaskInfo, BackendCompleteTaskSchedulerInfo

from ..stats import stats, volatile_stats
from ..config import config


router = APIRouter(tags=["Admin"], include_in_schema=not config["admin"]["hide_from_openapi"])
AUTH_TOKEN = config["admin"]["auth_token"]


class Worker(BaseModel):
    url: str
    token: Union[str, None]
    priority: int = 100
    timeout: int = 2
    proxy_data: bool = False
    maintenance: bool = False


@router.get("/admin/queue", summary="Get all running tasks on cluster")
def queue_info(authorization: Annotated[Union[str, None], Header()] = None) -> BackendCompleteTaskSchedulerInfo:
    """Get all running tasks on the entire cluster.

    Warning: This endpoint may impact workers' performance when used.

    If configured, this endpoint will require an authorization token."""
    if authorization != AUTH_TOKEN and AUTH_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid authorization token")
    _g, _ag, _qg = {}, {}, {}
    _t, _at, _qt = {}, {}, {}
    _a, _qa = [], []

    for k in client.BACKENDS.keys():
        data = client.get_all(k)
        if not data:
            continue

        for gk, gv in data["groups"].items():
            _g[gk] = {**_g.get(gk, {}), **gv}
        for agk, agv in data["active_groups"].items():
            _ag[agk] = {**_ag.get(agk, {}), **agv}
        for qgk, qgv in data["queued_groups"].items():
            _qg[qgk] = {**_qg.get(qgk, {}), **qgv}

        _t = {**_t, **data["tasks"]}
        _at = {**_at, **data["active_tasks"]}
        _qt = {**_qt, **data["queued_tasks"]}

        _a += data["actions"]
        _qa += data["queued_actions"]

    return BackendCompleteTaskSchedulerInfo(
        groups=_g,
        active_groups=_ag,
        queued_groups=_qg,
        tasks=_t,
        active_tasks=_at,
        queued_tasks=_qt,
        actions=_a,
        queued_actions=_qa
    )


@router.get("/admin/queue/all", summary="Get all running tasks on all workers")
def queue_info(authorization: Annotated[Union[str, None], Header()] = None) \
        -> dict[str, BackendCompleteTaskSchedulerInfo]:
    """Get all running tasks on all workers.

    Warning: This endpoint may impact workers' performance when used.

    If configured, this endpoint will require an authorization token."""
    if authorization != AUTH_TOKEN and AUTH_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid authorization token")
    _w = {}

    for k in client.BACKENDS.keys():
        data = client.get_all(k)
        if not data:
            continue

        _w[k] = BackendCompleteTaskSchedulerInfo(
            groups=data["groups"],
            active_groups=data["active_groups"],
            queued_groups=data["queued_groups"],
            tasks=data["tasks"],
            active_tasks=data["active_tasks"],
            queued_tasks=data["queued_tasks"],
            actions=data["actions"],
            queued_actions=data["queued_actions"]
        )

    return _w


@router.get("/admin/queue/{worker_id}", summary="Get all running tasks on a specific worker")
def queue_info(worker_id: str,
               authorization: Annotated[Union[str, None], Header()] = None) -> BackendCompleteTaskSchedulerInfo:
    """Get all running tasks on a specific worker.

    Warning: This endpoint may impact the worker's performance when used.

    If configured, this endpoint will require an authorization token."""
    if authorization != AUTH_TOKEN and AUTH_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid authorization token")
    if worker_id not in client.BACKENDS:
        raise HTTPException(status_code=404, detail="Unknown worker")

    data = client.get_all(worker_id)
    if not data:
        raise HTTPException(status_code=502, detail="Couldn't reach worker, please try again later")

    return BackendCompleteTaskSchedulerInfo(
        groups=data["groups"],
        active_groups=data["active_groups"],
        queued_groups=data["queued_groups"],
        tasks=data["tasks"],
        active_tasks=data["active_tasks"],
        queued_tasks=data["queued_tasks"],
        actions=data["actions"],
        queued_actions=data["queued_actions"]
    )


@router.delete("/admin/task/{task_id}", summary="Immediately cancel a running task")
def queue_cancel(task_id: str,
                 authorization: Annotated[Union[str, None], Header()] = None):
    """Find and cancel a running task on a worker.

    If configured, this endpoint will require an authorization token."""
    task = client.get_info(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    result = client.mark_failed(task_id)
    if not result:
        raise HTTPException(status_code=502, detail="Couldn't reach worker, please try again later")

    return result


@router.get("/admin/workers", summary="Get all registered workers")
def workers_list(authorization: Annotated[Union[str, None], Header()] = None) -> dict[str, Worker]:
    """Get all registered workers.

    If configured, this endpoint will require an authorization token."""
    if authorization != AUTH_TOKEN and AUTH_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid authorization token")
    return {k: Worker(url=v["url"], token=v["token"],
                      priority=v["priority"], timeout=v["timeout"],
                      proxy_data=v["proxy_data"], maintenance=v["maintenance"]) for k, v in client.BACKENDS.items()}


@router.put("/admin/workers/{worker_id}", summary="Add worker to backends")
def workers_add(worker_id: str,
                worker: Worker,
                authorization: Annotated[Union[str, None], Header()] = None) -> dict[str, Worker]:
    """Add worker to backends.

    If configured, this endpoint will require an authorization token."""
    if authorization != AUTH_TOKEN and AUTH_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid authorization token")
    config["frontend"]["backends"][worker_id] = {
        "url": worker.url,
        "token": worker.token,
        "priority": worker.priority,
        "timeout": worker.timeout,
        "proxy_data": worker.proxy_data,
        "maintenance": worker.maintenance
    }
    config.save()

    return {k: Worker(url=v["url"], token=v["token"],
                      priority=v["priority"], timeout=v["timeout"],
                      proxy_data=v["proxy_data"], maintenance=v["maintenance"]) for k, v in client.BACKENDS.items()}


@router.delete("/admin/workers/{worker_id}", summary="Remove worker from backends")
def workers_del(worker_id: str,
                authorization: Annotated[Union[str, None], Header()] = None) -> dict[str, Worker]:
    """Remove worker from backends.

    If configured, this endpoint will require an authorization token."""
    if authorization != AUTH_TOKEN and AUTH_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid authorization token")
    if worker_id not in client.BACKENDS:
        raise HTTPException(status_code=403, detail="Unknown worker")
    config["frontend"]["backends"].pop(worker_id)
    config.save()

    return {k: Worker(url=v["url"], token=v["token"],
                      priority=v["priority"], timeout=v["timeout"],
                      proxy_data=v["proxy_data"], maintenance=v["maintenance"]) for k, v in client.BACKENDS.items()}


@router.get("/admin/stats", summary="Statistics")
def get_stats(authorization: Annotated[Union[str, None], Header()] = None):
    if authorization != AUTH_TOKEN and AUTH_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid authorization token")
    return {"all_time": stats, "since_boot": volatile_stats}
