from typing import (Optional, List, Dict, TYPE_CHECKING, Callable)

from .rsp import DoneResponse, ResponseTypes

if TYPE_CHECKING:
    from .worker import Worker

from .typing import TaskFunc, LockerFunc


class Blueprint(object):
    """
    Blueprint acts as a registry for tasks, lockers, and configurations.
    It allows defining tasks independently of the Worker instance.
    Once a Worker is assigned, it proxies commands to it.
    """
    tasks: Dict[str, TaskFunc]
    broadcast_tasks: List[str]
    defrsps: Dict[str, ResponseTypes]
    lockers: Dict[str, LockerFunc]
    worker: Optional['Worker']

    def __init__(self) -> None:
        self.tasks = {}
        self.broadcast_tasks = []
        self.defrsps = {}
        self.lockers = {}
        self.worker = None

    def set_worker(self, worker: 'Worker') -> None:
        """Assigns a worker instance to this blueprint."""
        self.worker = worker

    async def add_func(
        self,
        func: str,
        task: TaskFunc,
        defrsp: ResponseTypes = DoneResponse(),
        locker: Optional[LockerFunc] = None,
    ) -> None:
        """
        Registers a task. If a worker is attached, registers directly on it.
        Otherwise, stores it locally in the blueprint.
        """
        if self.worker:
            await self.worker.add_func(func, task, defrsp, locker)
        else:
            self.tasks[func] = task
            self.defrsps[func] = defrsp
            if locker:
                self.lockers[func] = locker

    async def broadcast(
        self,
        func: str,
        task: TaskFunc,
        defrsp: ResponseTypes = DoneResponse(),
        locker: Optional[LockerFunc] = None,
    ) -> None:
        """
        Registers a broadcast task. These tasks are run on all workers.
        """
        if self.worker:
            await self.worker.broadcast(func, task, defrsp, locker)
        else:
            self.tasks[func] = task
            self.defrsps[func] = defrsp
            if locker:
                self.lockers[func] = locker
            self.broadcast_tasks.append(func)

    async def remove_func(self, func: str) -> None:
        """
        Removes a function from the worker.
        Raises an exception if no worker is currently assigned.
        """
        if self.worker:
            await self.worker.remove_func(func)
        else:
            raise Exception("Can't call remove_func before worker is set")

    # Decorator
    def func(
        self,
        func_name: str,
        broadcast: bool = False,
        defrsp: ResponseTypes = DoneResponse(),
        locker: Optional[LockerFunc] = None,
    ) -> Callable[[TaskFunc], TaskFunc]:
        """
        Decorator to register a function as a task.

        Usage:
            @bp.func('my_task')
            def my_task_handler(job):
                ...
        """

        def _func(task: TaskFunc) -> TaskFunc:
            self.tasks[func_name] = task
            self.defrsps[func_name] = defrsp

            if locker:
                self.lockers[func_name] = locker

            if broadcast:
                self.broadcast_tasks.append(func_name)

            return task

        return _func
