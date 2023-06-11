import threading
from time import sleep
from datetime import datetime, timedelta

from pathlib import Path
from os import listdir
from shutil import disk_usage

from .tasks import TaskScheduler

from ..config import config

SCHEDULER_EMPTY_WAIT = config["backend"]["scheduler_empty_wait"]
TASK_TTL = timedelta(seconds=config["backend"]["task_ttl"])
TASK_EMPTY_TTL = timedelta(seconds=config["backend"]["task_empty_ttl"])
CLEANUP_INTERVAL = config["backend"]["cleanup_interval"]
TEMP_PATH = Path(config["backend"]["temp_path"])
LIMITS = config["backend"]["limits"]

scheduler = TaskScheduler()


def _scheduler_loop():
    while True:
        scheduler.update_groups()
        if not scheduler.has_queue():
            sleep(SCHEDULER_EMPTY_WAIT)
        else:
            g = scheduler.next_group()
            t = g.next_task()
            a = t.next_action()
            try:
                a.run(t)
            except Exception as e:
                t.failed = True
                t.status = f"A critical error occurred while processing the task ({e})"


def _cleanup_loop():
    while True:
        now = datetime.utcnow()
        for g in scheduler.groups:
            for t in g.tasks.copy():
                if t.actions:
                    if now > (t.created_at + TASK_TTL):
                        t.get_cleanup_action().run(t)
                        t.delete_task()
                else:
                    if now > (t.created_at + TASK_EMPTY_TTL):
                        t.delete_task()
        for g in scheduler.groups.copy():
            if not g.tasks:
                g.delete_group()
        sleep(CLEANUP_INTERVAL)


scheduler_thread = threading.Thread(target=_scheduler_loop)
cleanup_thread = threading.Thread(target=_cleanup_loop)


def check_status():
    if not scheduler_thread.is_alive():
        return False
    if not cleanup_thread.is_alive():
        return False

    if LIMITS["max_groups"]:
        if len(scheduler.groups) >= LIMITS["max_groups"]:
            return False
    if LIMITS["max_active_groups"]:
        if len(scheduler.active_groups) >= LIMITS["max_active_groups"]:
            return False
    if LIMITS["max_tasks"]:
        if sum([len(g.tasks) for g in scheduler.groups]) >= LIMITS["max_tasks"]:
            return False
    if LIMITS["max_active_tasks"]:
        if sum([len(g.active_tasks) for g in scheduler.active_groups]) >= LIMITS["max_active_tasks"]:
            return False
    try:
        du = disk_usage(TEMP_PATH)
        total, used, free = round(du.total / 1000000, 2), round(du.used / 1000000, 2), round(du.free / 1000000, 2)
        if LIMITS["max_worker_space_mb"] or LIMITS["max_worker_space_pct"]:
            def _get_dir_size(root):
                c = 0
                for p in listdir(root):
                    po = Path(f"{root}/{p}")
                    if po.is_file():
                        c += po.stat().st_size
                    elif po.is_dir():
                        c += po.stat().st_size
                        c += _get_dir_size(po)
                return c
            worker_used = _get_dir_size(TEMP_PATH)

            if LIMITS["max_worker_space_mb"]:
                if round(worker_used / 1000000, 2) >= LIMITS["max_worker_space_mb"]:
                    return False
            if LIMITS["max_worker_space_pct"]:
                if (worker_used/du.total) * 100 >= LIMITS["max_worker_space_pct"]:
                    return False
        if LIMITS["max_used_space_mb"]:
            if used >= LIMITS["max_used_space_mb"]:
                return False
        if LIMITS["max_used_space_pct"]:
            if (du.used/du.total) * 100 >= LIMITS["max_used_space_pct"]:
                return False
        if LIMITS["min_free_space_mb"]:
            if free <= LIMITS["min_free_space_mb"]:
                return False
        if LIMITS["min_free_space_pct"]:
            if (du.free/du.total) * 100 <= LIMITS["min_free_space_pct"]:
                return False
    except FileNotFoundError:
        return False

    return True
