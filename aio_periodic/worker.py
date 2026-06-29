import asyncio
import logging
import signal
from time import time
from typing import List, Dict, Optional, Callable, cast
from concurrent.futures import Executor
from contextlib import suppress
from types import FrameType

# Third-party imports
from asyncio_pool import AioPool  # type: ignore

# Internal imports
from .job import Job
from .rsp import DoneResponse, FailResponse, SchedLaterResponse, ResponseTypes
from .types.utils import TYPE_WORKER
from .types.base_client import BaseClient, BaseCluster, is_success
from .types import command as cmd
from .types.agent import Agent
from .blueprint import Blueprint
from .typing import TaskFunc, LockerFunc

logger = logging.getLogger(__name__)


class GrabAgent(object):
    """
    Manages the state of a specific agent's 'GrabJob' request.
    Handles timeouts to prevent an agent from getting stuck waiting for a job.
    """
    agent: Agent
    sent_timer: int

    def __init__(self, agent: Agent) -> None:
        self.agent = agent
        self.sent_timer = 0

    async def safe_send(self) -> None:
        """Sends a GrabJob command and updates the timer."""
        try:
            await self.agent.send(cmd.GrabJob())
            self.sent_timer = int(time())
        except Exception as e:
            logger.exception(e)

    def is_timeout(self) -> bool:
        """Checks if the grab request has timed out (5 seconds)."""
        return self.sent_timer + 5 < int(time())

    async def send_assigned(self) -> None:
        """Acknowledges that a job has been assigned."""
        await self.agent.send(cmd.JobAssigned())

    async def send_unassigned(self) -> None:
        """Rejects an assigned job so server can re-dispatch it."""
        await self.agent.send(cmd.JobUnassigned())


class Worker(BaseClient):
    _tasks: Dict[str, TaskFunc]
    _broadcast_tasks: List[str]
    defrsps: Dict[str, ResponseTypes]
    lockers: Dict[str, LockerFunc]
    _pool: AioPool
    grab_queue: asyncio.Queue[GrabAgent]
    grab_agents: Dict[bytes, GrabAgent]
    executor: Optional[Executor]
    _stopping: bool

    def __init__(
        self,
        enabled_tasks: Optional[List[str]] = None,
        client_name: Optional[str] = None,
        client_token: Optional[str] = None,
    ) -> None:
        BaseClient.__init__(self, TYPE_WORKER, self._message_callback,
                            self._do_on_connected, None, client_name,
                            client_token)

        self.defrsps = {}
        self.lockers = {}
        self._tasks = {}
        self._broadcast_tasks = []
        self.enabled_tasks = list(enabled_tasks) if enabled_tasks else []

        # Initialize containers
        self.grab_agents = {}
        # Note: grab_queue and _pool are initialized in work()
        self.executor = None
        self._stopping = False

    def set_enable_tasks(self, enabled_tasks: List[str]) -> None:
        self.enabled_tasks = enabled_tasks

    def set_executor(self, executor: Executor) -> None:
        """Sets the executor for synchronous tasks."""
        self.executor = executor

    def is_enabled(self, func: str) -> bool:
        """Checks if a specific function is enabled on this worker."""
        if len(self.enabled_tasks) == 0:
            return True
        return func in self.enabled_tasks

    async def _do_on_connected(self) -> None:
        """
        Callback triggered when connected to the server.
        Re-registers all tasks.
        """
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

                # Retry delay
                await asyncio.sleep(1)

        await asyncio.sleep(1)

    async def _add_func(self, func: str) -> bool:
        """Internal method to register a function with the server."""
        if not self.is_enabled(func):
            return False
        logger.info(f'Add {func}')
        return cast(
            bool, await self.send_command_and_receive(
                cmd.CanDo(self._add_prefix_subfix(func)), is_success))

    async def add_func(
        self,
        func: str,
        task: TaskFunc,
        defrsp: Optional[ResponseTypes] = None,
        locker: Optional[LockerFunc] = None,
    ) -> bool:
        """Registers a new task/function."""
        if defrsp is None:
            defrsp = FailResponse()

        r = False
        if self.connected:
            r = await self._add_func(func)

        self._tasks[func] = task
        self.defrsps[func] = defrsp
        if locker:
            self.lockers[func] = locker
        return r

    async def _broadcast(self, func: str) -> bool:
        """Internal method to register a broadcast function."""
        if not self.is_enabled(func):
            return False
        logger.info(f'Broadcast {func}')
        return cast(
            bool, await self.send_command_and_receive(
                cmd.Broadcast(self._add_prefix_subfix(func)), is_success))

    async def broadcast(
        self,
        func: str,
        task: TaskFunc,
        defrsp: Optional[ResponseTypes] = None,
        locker: Optional[LockerFunc] = None,
    ) -> bool:
        """Registers a broadcast task (runs on all workers)."""
        if defrsp is None:
            defrsp = FailResponse()

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
        """Unregisters a task."""
        logger.info(f'Remove {func}')
        r = await self._cant_do(func)
        self._tasks.pop(func, None)
        self.defrsps.pop(func, None)
        self.lockers.pop(func, None)
        if func in self._broadcast_tasks:
            self._broadcast_tasks.remove(func)

        return cast(bool, r)

    async def _cant_do(self, func: str) -> bool:
        """Sends CantDo to server without mutating local task registry."""
        logger.info(f'CantDo {func}')
        return cast(
            bool, await self.send_command_and_receive(
                cmd.CantDo(self._add_prefix_subfix(func)), is_success))

    def stop(self) -> None:
        """Stops the worker loop from requesting new jobs."""
        self._stopping = True

    async def next_grab(self) -> None:
        """
        Checks the grab queue. If the pool is not full, sends GrabJob requests
        for idle agents.
        """
        if not self.connected or self._stopping:
            return

        if self._pool.is_full:
            return

        # Check existing idle agents in the queue
        for _ in range(self.grab_queue.qsize()):
            agent = await self.grab_queue.get()
            await self.grab_queue.put(agent)

            # If agent hasn't sent a request recently, send one now
            if agent.is_timeout():
                await agent.safe_send()
                # Only grab one per cycle to balance load/requests
                break

    async def work(self, size: int) -> None:
        """Starts the worker loop with a specified concurrency size."""
        self._stopping = False
        self._pool = AioPool(size=size)
        agents = [self.agent() for _ in range(size)]
        self.grab_queue = asyncio.Queue()

        for agent in agents:
            agent.__enter__()
            item = GrabAgent(agent)
            await self.grab_queue.put(item)
            self.grab_agents[agent.msgid] = item

        # Main worker loop
        while not self._stopping:
            await self.next_grab()
            await asyncio.sleep(1)

    async def graceful_shutdown(
        self,
        funcs: Optional[List[str]] = None,
        close_client: bool = True,
    ) -> None:
        """
        Graceful shutdown flow:
        1) stop accepting new jobs
        2) send CantDo for all functions
        3) wait for in-flight jobs to finish
        4) close network client
        """
        self.stop()

        def_funcs = list(self._tasks.keys())
        cant_do_funcs = funcs if funcs is not None else def_funcs
        for func in cant_do_funcs:
            try:
                await self._cant_do(func)
            except Exception as e:
                logger.exception(e)

        if hasattr(self, '_pool'):
            await self._pool.join()

        if close_client:
            self.close()

    async def work_until_shutdown(
        self,
        size: int,
        signal_names: Optional[List[str]] = None,
    ) -> None:
        """
        Runs work loop and installs graceful shutdown handlers for signals.
        Default signals: SIGTERM and SIGHUP.
        """
        loop = asyncio.get_running_loop()
        def_signal_names = ['SIGTERM', 'SIGHUP']
        selected_signals = signal_names if signal_names else def_signal_names
        shutdown_task = None

        def start_shutdown(sig_name: str) -> None:
            nonlocal shutdown_task
            if shutdown_task is None:
                logger.info(f'Received {sig_name}, graceful shutdown started')
                shutdown_task = asyncio.create_task(self.graceful_shutdown())

        def mk_loop_signal_handler(sig_name: str) -> Callable[[], None]:

            def handler() -> None:
                start_shutdown(sig_name)

            return handler

        def mk_sync_signal_handler(
            sig_name: str,
        ) -> Callable[[int, Optional[FrameType]], None]:

            def handler(_signum: int, _frame: Optional[FrameType]) -> None:
                loop.call_soon_threadsafe(start_shutdown, sig_name)

            return handler

        registered_signals = []
        for sig_name in selected_signals:
            sig = getattr(signal, sig_name, None)
            if sig is None:
                continue
            registered_signals.append(sig)
            try:
                loop.add_signal_handler(sig, mk_loop_signal_handler(sig_name))
            except NotImplementedError:
                signal.signal(sig, mk_sync_signal_handler(sig_name))

        try:
            await self.work(size)
        finally:
            for sig in registered_signals:
                with suppress(NotImplementedError):
                    loop.remove_signal_handler(sig)

        if shutdown_task:
            await shutdown_task

    async def _message_callback(self, payload: bytes, msgid: bytes) -> None:
        """Callback when the server assigns a job."""
        agent = self.grab_agents.get(msgid)
        if not agent:
            logger.error(f'Grab agent not found for msgid={msgid!r}')
            return

        # Reject immediately when saturated, do not queue execution.
        if self._stopping or self._pool.is_full:
            try:
                await agent.send_unassigned()
            except Exception as e:
                logger.exception(e)
            return

        # Immediately try to grab next job while processing this one
        await self.next_grab()

        # Spawn the task execution in the pool.
        try:
            self._pool.spawn_n(self.run_task(payload, msgid))
        except Exception as e:
            logger.exception(e)

    async def run_task(self, payload: bytes, msgid: bytes) -> None:
        """Decodes the job payload and runs the processing logic."""
        agent = self.grab_agents.get(msgid)
        if not agent:
            logger.error(f'Grab agent not found for msgid={msgid!r}')
            return

        job: Optional[Job] = None
        try:
            await agent.send_assigned()
            # Payload[0] is command, Payload[1:] is Job Handle/Args
            job = Job(payload[1:], self)
            await self.process_job(job)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.exception(e)
            if job and not job.finished:
                with suppress(Exception):
                    await job.fail()
        finally:
            # Re-queue agent for next grab
            if self.connected and not self._stopping:
                await agent.safe_send()

    async def process_job(self, job: Job) -> None:
        """Determines logic for execution (locking, missing task, etc.)."""
        task = self._tasks.get(job.func_name)
        if not task:
            await self.remove_func(job.func_name)
            await job.fail()
        else:

            async def process() -> None:
                await self._process_job(job, task)

            # Handle Locking if configured
            locker = self.lockers.get(job.func_name)
            if locker:
                locker_name, count = locker(job)
                if locker_name:
                    await job.with_lock(locker_name, count, process)
                    return

            await process()

    async def _process_job(self, job: Job, task: TaskFunc) -> None:
        """Executes the user-defined task function (Sync or Async)."""
        try:
            if asyncio.iscoroutinefunction(task):
                ret = await task(job)
            else:
                # Optimized: directly await run_in_executor
                if self.executor:
                    loop = asyncio.get_running_loop()
                    ret = await loop.run_in_executor(self.executor, task, job)
                else:
                    # Run sync blocking (not recommended without executor)
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

    # Decorator
    def func(
        self,
        func_name: str,
        broadcast: bool = False,
        defrsp: Optional[ResponseTypes] = None,
        locker: Optional[LockerFunc] = None,
    ) -> Callable[[TaskFunc], TaskFunc]:
        """Decorator to register a function."""
        if defrsp is None:
            defrsp = FailResponse()

        def _func(task: TaskFunc) -> TaskFunc:
            self._tasks[func_name] = task
            self.defrsps[func_name] = defrsp
            if locker:
                self.lockers[func_name] = locker
            if broadcast:
                self._broadcast_tasks.append(func_name)
            return task

        return _func

    def blueprint(self, app: Blueprint) -> None:
        """Registers tasks from a Blueprint."""
        app.set_worker(self)
        self._tasks.update(app.tasks)
        self.defrsps.update(app.defrsps)
        self.lockers.update(app.lockers)

        for btsk in app.broadcast_tasks:
            self._broadcast_tasks.append(btsk)


class WorkerCluster(BaseCluster):
    """
    Manages a cluster of Workers to distribute load or connect to
    multiple Periodic servers.
    """

    def __init__(self,
                 entrypoints: List[str],
                 enabled_tasks: Optional[List[str]] = None,
                 client_name: Optional[str] = None,
                 client_token: Optional[str] = None) -> None:
        BaseCluster.__init__(self,
                             Worker,
                             entrypoints,
                             enabled_tasks=enabled_tasks,
                             client_name=client_name,
                             client_token=client_token)

    def set_enable_tasks(self, enabled_tasks: List[str]) -> None:
        self.run_sync('set_enable_tasks', enabled_tasks)

    def is_enabled(self, func: str) -> bool:

        def reduce(acc: bool, a: bool) -> bool:
            return acc and a

        return cast(
            bool,
            self.run_sync('is_enabled', func, reduce=reduce, initialize=True))

    async def add_func(
        self,
        func: str,
        task: TaskFunc,
        defrsp: Optional[ResponseTypes] = None,
        locker: Optional[LockerFunc] = None,
    ) -> None:
        await self.run('add_func', func, task, defrsp, locker)

    async def broadcast(
        self,
        func: str,
        task: TaskFunc,
        defrsp: Optional[ResponseTypes] = None,
        locker: Optional[LockerFunc] = None,
    ) -> None:
        await self.run('broadcast', func, task, defrsp, locker)

    async def remove_func(self, func: str) -> None:
        await self.run('remove_func', func)

    async def work(self, size: int) -> None:
        await self.run('work', size)

    async def graceful_shutdown(
        self,
        funcs: Optional[List[str]] = None,
        close_client: bool = True,
    ) -> None:
        await self.run('graceful_shutdown', funcs, close_client)

    # Decorator
    def func(
        self,
        func_name: str,
        broadcast: bool = False,
        defrsp: Optional[ResponseTypes] = None,
        locker: Optional[LockerFunc] = None,
    ) -> Callable[[TaskFunc], TaskFunc]:
        if defrsp is None:
            defrsp = FailResponse()

        def _func(task: TaskFunc) -> TaskFunc:

            def reduce(ret: bool, call: Callable[[TaskFunc],
                                                 TaskFunc]) -> bool:
                call(task)
                return ret

            self.run_sync('func',
                          func_name,
                          broadcast,
                          defrsp,
                          locker,
                          reduce=reduce,
                          initialize=True)
            return task

        return _func

    def blueprint(self, app: Blueprint) -> None:
        self.run_sync('blueprint', app)
