from .job import Job
from .rsp import DoneResponse, FailResponse, SchedLaterResponse
from .types.utils import TYPE_WORKER
from .types.base_client import BaseClient, BaseCluster, is_success
from .types import command as cmd
from .types.agent import Agent
import asyncio
from asyncio_pool import AioPool
from time import time
from typing import List, Dict, Any, Optional, Callable, cast, Coroutine
from .blueprint import Blueprint
from concurrent.futures import Executor

import logging

logger = logging.getLogger(__name__)


class GrabAgent(object):
    agent: Agent
    sent_timer: int

    def __init__(self, agent: Agent) -> None:
        self.agent = agent
        self.sent_timer = 0

    async def safe_send(self) -> None:
        try:
            await self.agent.send(cmd.GrabJob())
            self.sent_timer = int(time())
        except Exception as e:
            logger.exception(e)

    def is_timeout(self) -> bool:
        return self.sent_timer + 5 < int(time())


class Worker(BaseClient):
    _tasks: Dict[str, Callable[[Job], Coroutine[Any, Any, Any]]
                 | Callable[[Job], Any]]
    _broadcast_tasks: List[str]
    defrsps: Dict[str, DoneResponse | FailResponse | SchedLaterResponse]
    lockers: Dict[str, Callable[[Job], tuple[str, int]]]
    _pool: AioPool
    grab_queue: asyncio.Queue[GrabAgent]
    grab_agents: Dict[bytes, GrabAgent]
    executor: Executor | None

    def __init__(self, enabled_tasks: List[str] = []) -> None:
        BaseClient.__init__(self, TYPE_WORKER, self._message_callback,
                            self._do_on_connected)

        self.defrsps = {}
        self.lockers = {}
        self._tasks = {}
        self._broadcast_tasks = []
        self.enabled_tasks = enabled_tasks
        # self._pool = None

        self.grab_agents = {}
        # self.grab_queue = None
        self.executor = None

    def set_enable_tasks(self, enabled_tasks: List[str]) -> None:
        self.enabled_tasks = enabled_tasks

    def set_executor(self, executor: Executor) -> None:
        '''executer for sync process'''
        self.executor = executor

    def is_enabled(self, func: str) -> bool:
        if len(self.enabled_tasks) == 0:
            return True
        return func in self.enabled_tasks

    async def _do_on_connected(self) -> None:
        for func in self._tasks.keys():
            if not self.is_enabled(func):
                continue

            while True:
                r = False
                if func in self._broadcast_tasks:
                    r = await self._broadcast(func)
                else:
                    r = await self._add_func(func)

                if r:
                    break

                await asyncio.sleep(1)

        await asyncio.sleep(1)

    async def _add_func(self, func: str) -> bool:
        if not self.is_enabled(func):
            return False
        logger.info(f'Add {func}')
        return cast(
            bool, await self.send_command_and_receive(
                cmd.CanDo(self._add_prefix_subfix(func)), is_success))

    async def add_func(
        self,
        func: str,
        task: Callable[[Job], Coroutine[Any, Any, Any]]
        | Callable[[Job], Any],
        defrsp: DoneResponse | FailResponse
        | SchedLaterResponse = FailResponse(),
        locker: Optional[Callable[[Job], tuple[str, int]]] = None,
    ) -> bool:
        r = False
        if self.connected:
            r = await self._add_func(func)

        self._tasks[func] = task
        self.defrsps[func] = defrsp
        if locker:
            self.lockers[func] = locker
        return r

    async def _broadcast(self, func: str) -> bool:
        if not self.is_enabled(func):
            return False
        logger.info(f'Broadcast {func}')
        return cast(
            bool, await self.send_command_and_receive(
                cmd.Broadcast(self._add_prefix_subfix(func)), is_success))

    async def broadcast(
        self,
        func: str,
        task: Callable[[Job], Coroutine[Any, Any, Any]]
        | Callable[[Job], Any],
        defrsp: DoneResponse | FailResponse
        | SchedLaterResponse = FailResponse(),
        locker: Optional[Callable[[Job], tuple[str, int]]] = None,
    ) -> bool:
        r = False
        if self.connected:
            r = await self._broadcast(func)

        self._tasks[func] = task
        self.defrsps[func] = defrsp
        if locker:
            self.lockers[func] = locker
        self._broadcast_tasks.append(func)

        return r

    async def remove_func(self, func: str) -> bool:
        logger.info(f'Remove {func}')
        r = await self.send_command_and_receive(
            cmd.CantDo(self._add_prefix_subfix(func)), is_success)
        self._tasks.pop(func, None)
        self.defrsps.pop(func, None)
        self.lockers.pop(func, None)
        if func in self._broadcast_tasks:
            self._broadcast_tasks.remove(func)

        return cast(bool, r)

    async def next_grab(self) -> None:
        if self._pool.is_full:
            return

        for _ in range(self.grab_queue.qsize()):
            agent = await self.grab_queue.get()
            await self.grab_queue.put(agent)
            if agent.is_timeout():
                await agent.safe_send()
                break

    async def work(self, size: int) -> None:
        self._pool = AioPool(size=size)
        agents = [self.agent() for _ in range(size)]
        self.grab_queue = asyncio.Queue()

        for agent in agents:
            agent.__enter__()
            item = GrabAgent(agent)
            await self.grab_queue.put(item)
            self.grab_agents[agent.msgid] = item

        while True:
            await self.next_grab()
            await asyncio.sleep(1)

    async def _message_callback(self, payload: bytes, msgid: bytes) -> None:
        await self.next_grab()
        self._pool.spawn_n(self.run_task(payload, msgid))

    async def run_task(self, payload: bytes, msgid: bytes) -> None:
        try:
            job = Job(payload[1:], self)
            await self.process_job(job)
        except asyncio.exceptions.CancelledError:
            pass
        finally:
            agent = self.grab_agents[msgid]
            await agent.safe_send()

    async def process_job(self, job: Job) -> None:
        task = self._tasks.get(job.func_name)
        if not task:
            await self.remove_func(job.func_name)
            await job.fail()
        else:

            async def process() -> None:
                await self._process_job(job, task)

            locker = self.lockers.get(job.func_name)
            if locker:
                locker_name, count = locker(job)
                if locker_name:
                    await job.with_lock(locker_name, count, process)
                    return

            await process()

    async def _process_job(
        self, job: Job, task: Callable[[Job], Coroutine[Any, Any, Any]]
        | Callable[[Job], Any]
    ) -> None:
        try:
            if asyncio.iscoroutinefunction(task):
                ret = await task(job)
            else:
                if self.executor:
                    loop = asyncio.get_running_loop()
                    t = loop.run_in_executor(self.executor, task, job)
                    await asyncio.wait([t])
                    ret = t.result()
                else:
                    ret = task(job)

        except Exception as e:
            logger.exception(e)
            ret = self.defrsps.get(job.func_name, FailResponse())

        if not job.finished:
            if isinstance(ret, DoneResponse):
                await job.done(ret.buf)
            elif isinstance(ret, FailResponse):
                await job.fail()
            elif isinstance(ret, SchedLaterResponse):
                await job.sched_later(ret.delay, ret.count)
            else:
                await job.done(ret)

    # decorator
    def func(
        self,
        func_name: str,
        broadcast: bool = False,
        defrsp: DoneResponse | FailResponse
        | SchedLaterResponse = FailResponse(),
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
            self._tasks[func_name] = task
            self.defrsps[func_name] = defrsp
            if locker:
                self.lockers[func_name] = locker
            if broadcast:
                self._broadcast_tasks.append(func_name)
            return task

        return _func

    def blueprint(self, app: Blueprint) -> None:
        app.set_worker(self)
        self._tasks.update(app.tasks)
        self.defrsps.update(app.defrsps)
        self.lockers.update(app.lockers)

        for btsk in app.broadcast_tasks:
            self._broadcast_tasks.append(btsk)


class WorkerCluster(BaseCluster):

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        BaseCluster.__init__(self, Worker, *args, **kwargs)

    def set_enable_tasks(self, enabled_tasks: List[str]) -> None:
        self.run_sync('set_enable_tasks', enabled_tasks)

    def is_enabled(self, func: str) -> bool:

        def reduce(acc: bool, a: bool) -> bool:
            return acc and a

        return cast(
            bool,
            self.run_sync(
                'is_enabled',
                func,
                reduce=reduce,
                initialize=True,
            ))

    async def add_func(
        self,
        func: str,
        task: Callable[[Job], Coroutine[Any, Any, Any]]
        | Callable[[Job], Any],
        defrsp: DoneResponse | FailResponse
        | SchedLaterResponse = FailResponse(),
        locker: Optional[Callable[[Job], tuple[str, int]]] = None,
    ) -> None:
        await self.run('add_func', func, task, defrsp, locker)

    async def broadcast(
        self,
        func: str,
        task: Callable[[Job], Coroutine[Any, Any, Any]]
        | Callable[[Job], Any],
        defrsp: DoneResponse | FailResponse
        | SchedLaterResponse = FailResponse(),
        locker: Optional[Callable[[Job], tuple[str, int]]] = None,
    ) -> None:
        await self.run('broadcast', func, task, defrsp, locker)

    async def remove_func(self, func: str) -> None:
        await self.run('remove_func', func)

    async def work(self, size: int) -> None:
        await self.run('work', size)

    # decorator
    def func(
        self,
        func_name: str,
        broadcast: bool = False,
        defrsp: DoneResponse | FailResponse
        | SchedLaterResponse = FailResponse(),
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

            def reduce(_: Any, call: Any) -> None:
                call(task)

            self.run_sync('func',
                          func_name,
                          broadcast,
                          defrsp,
                          locker,
                          reduce=reduce)

            return task

        return _func

    def blueprint(self, app: Blueprint) -> None:
        self.run_sync('blueprint', app)
