from datetime import datetime
from .actions import ActionBase, DefaultCleanupAction
from typing import Union


class TaskScheduler:
    instance = None

    def __init__(self, groups=None):
        self.groups: list[TaskGroup] = groups or []
        self.active_groups: list[TaskGroup] = groups or []
        self.queued_groups: list[TaskGroup] = groups or []
        TaskScheduler.instance = self

    def __repr__(self):
        return f"TaskScheduler(groups={len(self.groups)}," \
               f"active={len(self.active_groups)}, queued={len(self.queued_groups)})"

    def add_group(self, group):
        if group not in self.groups:
            self.groups.append(group)
            self.active_groups.append(group)
            self.queued_groups.append(group)
        group.parent = self

    def remove_group(self, group):
        if group in self.groups:
            self.groups.remove(group)
        if group in self.active_groups:
            self.active_groups.remove(group)
        if group in self.queued_groups:
            self.queued_groups.remove(group)
        group.parent = None

    def next_group(self):
        if not self.queued_groups:
            raise EOFError("No groups in queue")
        _g = self.queued_groups.pop(0)
        if not self.queued_groups:
            self.queued_groups = self.active_groups.copy()
        return _g

    def has_queue(self):
        return any([True for g in self.active_groups if g.has_queue()])

    def update_groups(self):
        for g in self.groups:
            g.update_tasks()
            if g.has_queue():
                if g not in self.active_groups:
                    self.active_groups.append(g)
                if g not in self.queued_groups:
                    self.queued_groups.append(g)
            else:
                if g in self.active_groups:
                    self.active_groups.remove(g)
                if g in self.queued_groups:
                    self.queued_groups.remove(g)

    def destroy(self):
        for g in self.groups.copy():
            self.groups.remove(g)
            g.delete_group()
        TaskScheduler.instance = None


class TaskGroup:
    instances = {}

    def __init__(self, uid, tasks=None):
        self.uid: str = uid

        self.tasks: list[Task] = tasks or []
        self.active_tasks: list[Task] = tasks or []
        self.queued_tasks: list[Task] = tasks or []

        self.parent: Union[TaskScheduler, None] = None
        TaskGroup.instances[uid] = self

    def __repr__(self):
        return f"TaskGroup(uid={self.uid}, tasks={len(self.tasks)}," \
               f"active={len(self.active_tasks)}, queued={len(self.queued_tasks)})"

    def add_task(self, task):
        if task not in self.tasks:
            self.tasks.append(task)
            self.active_tasks.append(task)
            self.queued_tasks.append(task)
        task.parent = self

    def remove_task(self, task):
        if task in self.tasks:
            self.tasks.remove(task)
        if task in self.active_tasks:
            self.active_tasks.remove(task)
        if task in self.queued_tasks:
            self.queued_tasks.remove(task)
        task.parent = None

    def next_task(self):
        if not self.queued_tasks:
            raise EOFError("No tasks in queue")
        _t = self.queued_tasks.pop(0)
        if not self.queued_tasks:
            self.queued_tasks = self.active_tasks.copy()
        return _t

    def has_queue(self):
        return any([True for t in self.active_tasks if not t.completed and not t.failed])

    def update_tasks(self):
        for t in self.tasks:
            if not t.queued_actions or t.failed:
                if t in self.active_tasks:
                    self.active_tasks.remove(t)
                if t in self.queued_tasks:
                    self.queued_tasks.remove(t)
            else:
                if t not in self.active_tasks:
                    self.active_tasks.append(t)
                if t not in self.queued_tasks:
                    self.queued_tasks.append(t)

    def delete_group(self):
        for t in self.tasks.copy():
            self.tasks.remove(t)
            t.delete_task()
        TaskGroup.instances.pop(self.uid)
        if self.parent:
            self.parent.remove_group(self)

    @classmethod
    def get_group(cls, uid):
        if uid in TaskGroup.instances:
            return TaskGroup.instances[uid]
        return cls(uid)


class Task:
    instances = {}

    def __init__(self, uid, kind=None, actions=None):
        self.uid: str = uid
        self.kind: Union[str, None] = kind

        self.actions: list[ActionBase] = actions or []
        self.queued_actions: list[ActionBase] = actions or []
        self.cleanup_action: Union[ActionBase, None] = None

        self.started = False
        self.completed = False
        self.failed = False

        self.status: Union[str, None] = None
        self.status_override: Union[str, None] = None
        self.result: Union[str, None] = None

        self.created_at: datetime = datetime.utcnow()

        self.parent: Union[TaskGroup, None] = None
        Task.instances[uid] = self

    def __repr__(self):
        return f"Task(uid={self.uid}, actions={len(self.actions)}, queued={len(self.queued_actions)}, " \
               f"started={self.started}, completed={self.completed}, failed={self.failed}, " \
               f"status={self.status}, progress={self.progress})"

    @property
    def progress(self):
        if not self.actions:
            return 0
        _t = len(self.actions)
        _q = len(self.queued_actions)
        return round(((_t - _q)/_t)*100)

    def add_action(self, action):
        if action not in self.actions:
            self.actions.append(action)
            self.queued_actions.append(action)

    def remove_action(self, action):
        if action in self.actions:
            self.actions.remove(action)
        if action in self.queued_actions:
            self.queued_actions.remove(action)

    def next_action(self):
        if not self.queued_actions:
            raise EOFError("No actions left in queue")
        return self.queued_actions.pop(0)

    def get_cleanup_action(self):
        if not self.cleanup_action:
            return DefaultCleanupAction()
        return self.cleanup_action

    def delete_task(self):
        Task.instances.pop(self.uid)
        if self.parent:
            self.parent.remove_task(self)

    @classmethod
    def get_task(cls, uid):
        if uid in Task.instances:
            return Task.instances[uid]
        return cls(uid)
