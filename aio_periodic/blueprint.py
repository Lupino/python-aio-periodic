from .rsp import DoneResponse, FailResponse, SchedLaterResponse
from typing import Optional, List, Dict, Any, TYPE_CHECKING, Optional, Callable, \
    Coroutine

from .job import Job

if TYPE_CHECKING:
    from .worker import Worker


class Blueprint(object):
    tasks: Dict[str, Callable[[Job], Coroutine[Any, Any, Any]]
                | Callable[[Job], Any]]
    broadcast_tasks: List[str]
    defrsps: Dict[str, DoneResponse | FailResponse | SchedLaterResponse]
    lockers: Dict[str, Callable[[Job], tuple[str, int]]]
    worker: Optional['Worker']

    def __init__(self) -> None:
        self.tasks = {}
        self.broadcast_tasks = []
        self.defrsps = {}
        self.lockers = {}
        self.worker = None

    def set_worker(self, worker: 'Worker') -> None:
        self.worker = worker

    async def add_func(
        self,
        func: str,
        task: Callable[[Job], Coroutine[Any, Any, Any]]
        | Callable[[Job], Any],
        defrsp: DoneResponse | FailResponse
        | SchedLaterResponse = DoneResponse(),
        locker: Optional[Callable[[Job], tuple[str, int]]] = None,
    ) -> None:
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
        task: Callable[[Job], Coroutine[Any, Any, Any]]
        | Callable[[Job], Any],
        defrsp: DoneResponse | FailResponse
        | SchedLaterResponse = DoneResponse(),
        locker: Optional[Callable[[Job], tuple[str, int]]] = None,
    ) -> None:
        if self.worker:
            await self.worker.broadcast(func, task, defrsp, locker)
        else:
            self.tasks[func] = task
            self.defrsps[func] = defrsp
            if locker:
                self.lockers[func] = locker
            self.broadcast_tasks.append(func)

    async def remove_func(self, func: str) -> None:
        if self.worker:
            await self.worker.remove_func(func)
        else:
            raise Exception("Can't call remove_func before apply")

    # decorator
    def func(
        self,
        func_name: str,
        broadcast: bool = False,
        defrsp: DoneResponse | FailResponse
        | SchedLaterResponse = DoneResponse(),
        locker: Optional[Callable[[Job], tuple[str, int]]] = None,
    ) -> Callable[
        [
            Callable[
                [Job],
                Coroutine[Any, Any, Any],
            ] | Callable[
                [Job],
                Any,
            ],
        ],
            Callable[
                [Job],
                Coroutine[Any, Any, Any],
            ] | Callable[
                [Job],
                Any,
            ],
    ]:

        def _func(
            task: Callable[
                [Job],
                Coroutine[Any, Any, Any],
            ]
            | Callable[
                [Job],
                Any,
            ]
        ) -> Callable[
            [Job],
                Coroutine[Any, Any, Any],
        ] | Callable[
            [Job],
                Any,
        ]:
            self.tasks[func_name] = task
            self.defrsps[func_name] = defrsp
            if locker:
                self.lockers[func_name] = locker
            if broadcast:
                self.broadcast_tasks.append(func_name)

            return task

        return _func
