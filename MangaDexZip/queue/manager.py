import threading
from time import sleep
from datetime import datetime, timedelta

from .tasks import TaskScheduler

from ..config import config

SCHEDULER_EMPTY_WAIT = config["backend"]["scheduler_empty_wait"]
TASK_TTL = timedelta(seconds=config["backend"]["task_ttl"])
TASK_EMPTY_TTL = timedelta(seconds=config["backend"]["task_empty_ttl"])
CLEANUP_INTERVAL = config["backend"]["cleanup_interval"]

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
