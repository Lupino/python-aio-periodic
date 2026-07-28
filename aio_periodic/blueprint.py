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
        # Keep registrations until Worker.blueprint() attaches this blueprint.
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
        defrsp: Optional[ResponseTypes] = None,
        locker: Optional[LockerFunc] = None,
    ) -> None:
        """
        Registers a task. If a worker is attached, registers directly on it.
        Otherwise, stores it locally in the blueprint.
        """
        if defrsp is None:
            defrsp = DoneResponse()

        if self.worker:
            # A bound blueprint must update the live worker immediately.
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
        defrsp: Optional[ResponseTypes] = None,
        locker: Optional[LockerFunc] = None,
    ) -> None:
        """
        Registers a broadcast task. These tasks are run on all workers.
        """
        if defrsp is None:
            defrsp = DoneResponse()

        if self.worker:
            # Broadcast registration follows the same live-update path as tasks.
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

    # Decorators run during module import, before a worker is normally attached.
    def func(
        self,
        func_name: str,
        broadcast: bool = False,
        defrsp: Optional[ResponseTypes] = None,
        locker: Optional[LockerFunc] = None,
    ) -> Callable[[TaskFunc], TaskFunc]:
        """
        Decorator to register a function as a task.

        Usage:
            @bp.func('my_task')
            def my_task_handler(job):
                ...
        """

        if defrsp is None:
            defrsp = DoneResponse()

        def _func(task: TaskFunc) -> TaskFunc:
            # Decorated tasks are collected before Worker.blueprint() transfers
            # the blueprint state to its reconnect-aware task registry.
            self.tasks[func_name] = task
            self.defrsps[func_name] = defrsp

            if locker:
                self.lockers[func_name] = locker

            if broadcast:
                self.broadcast_tasks.append(func_name)

            return task

        return _func
