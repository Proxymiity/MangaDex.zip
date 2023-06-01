import requests
from requests.exceptions import RequestException
from json.decoder import JSONDecodeError

from datetime import datetime, timedelta

import random

from ..config import config

BACKENDS = config["frontend"]["backends"]
TASK_CACHE_TTL = timedelta(seconds=config["frontend"]["task_cache_ttl"])

task_cache = {}


class WorkerProxyDisabledError(Exception):
    pass


class WorkerFileNotReadyError(Exception):
    pass


class WorkerTaskNotSupportedError(Exception):
    pass


def reload_workers():
    config.reload()
    for k in BACKENDS.copy():
        if k not in config["frontend"]["backends"]:
            BACKENDS.pop(k)
    for k, v in config["frontend"]["backends"].items():
        BACKENDS[k] = v


def available_workers():
    return {k: v for k, v in BACKENDS.items() if not v["maintenance"]}


def select_worker(uid):
    return available_workers().get(uid)


def select_worker_random():
    return random.choice(tuple(available_workers().keys()))


def select_worker_auto():
    workers = {}
    for k in available_workers().keys():
        q = query(k)
        if q:
            workers[k] = q["active_groups"]
    if not workers:
        return None
    lowest_workers = [k for k, v in workers.items() if v == min(workers.values())]
    lowest_workers.sort(key=lambda x: BACKENDS[x]["priority"], reverse=True)
    return lowest_workers[0]


def query(worker_uid):
    worker = BACKENDS[worker_uid]
    try:
        req = requests.get(f"{worker['url']}/queue/back",
                           headers={
                               "Authorization": worker["token"]
                           },
                           timeout=worker["timeout"])
        if req.status_code != 200:
            return None
        return req.json()
    except (RequestException, JSONDecodeError):
        return None


def append(worker_uid, task_type, task_data, task_opt_data, task_group):
    worker = BACKENDS[worker_uid]
    try:
        req = requests.post(f"{worker['url']}/queue/back/new",
                            json={
                                "type": task_type,
                                "data": task_data,
                                "opt_data": task_opt_data,
                                "group": task_group
                            },
                            headers={
                                "Authorization": worker["token"]
                            },
                            timeout=worker["timeout"])
        if req.status_code != 200:
            return None
        task = req.json()
        task_cache[task["task_id"]] = (worker_uid, datetime.utcnow())
        return task
    except (RequestException, JSONDecodeError):
        return None


def get_info(task_uid):
    if task_uid in task_cache:
        if (task_cache[task_uid][1] + TASK_CACHE_TTL) > datetime.utcnow():
            _ti = _get_info_from_worker(task_cache[task_uid][0], task_uid)
            if _ti:
                task_cache[task_uid] = (task_cache[task_uid][0], datetime.utcnow())
                return _ti
    for k in BACKENDS.keys():
        _ti = _get_info_from_worker(k, task_uid)
        if _ti:
            task_cache[task_uid] = (k, datetime.utcnow())
            return _ti
    return None


def get_all(worker_uid):
    worker = BACKENDS[worker_uid]
    try:
        req = requests.get(f"{worker['url']}/queue/back/all",
                           headers={
                               "Authorization": worker["token"]
                           },
                           timeout=worker["timeout"])
        if req.status_code != 200:
            return None
        return req.json()
    except (RequestException, JSONDecodeError):
        return None


def _get_info_from_worker(worker_uid, task_uid):
    worker = BACKENDS[worker_uid]
    try:
        req = requests.get(f"{worker['url']}/queue/back/{task_uid}",
                           headers={
                                "Authorization": worker["token"]
                            },
                           timeout=worker["timeout"])
        if req.status_code != 200:
            return None
        return req.json()
    except (RequestException, JSONDecodeError):
        return None


def mark_failed(task_uid):
    if task_uid not in task_cache:
        d = get_info(task_uid)
        if not d:
            raise FileNotFoundError("Task not found")
    worker = BACKENDS[task_cache[task_uid][0]]
    try:
        req = requests.delete(f"{worker['url']}/queue/back/{task_uid}",
                              headers={
                                  "Authorization": worker["token"]
                              },
                              timeout=worker["timeout"])
        if req.status_code != 200:
            return None
        return req.json()
    except (RequestException, JSONDecodeError):
        return None


def proxy_data(task_uid):
    if task_uid not in task_cache:
        d = get_info(task_uid)
        if not d:
            raise FileNotFoundError("Task not found")
    worker = BACKENDS[task_cache[task_uid][0]]
    if not worker["proxy_data"]:
        raise WorkerProxyDisabledError("Worker does not allow proxying")

    with requests.get(f"{worker['url']}/queue/back/{task_uid}/data", headers={"Authorization": worker["token"]}) as r:
        if r.status_code == 404:
            raise FileNotFoundError("Task not found")
        if r.status_code == 403:
            raise WorkerFileNotReadyError("Worker file not ready")
        if r.status_code == 503:
            raise WorkerTaskNotSupportedError("Worker unknown task kind")
        r.raise_for_status()
        return r
