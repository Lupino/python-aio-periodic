from .job import Job
from .rsp import DoneResponse, FailResponse, SchedLaterResponse
from .types.utils import TYPE_WORKER
from .types.base_client import BaseClient, BaseCluster
from .types import command as cmd
import asyncio
from asyncio_pool import AioPool
from time import time

import logging

logger = logging.getLogger(__name__)


class GrabAgent(object):
    def __init__(self, agent):
        self.agent = agent
        self.sent_timer = 0

    async def safe_send(self):
        try:
            await self.agent.send(cmd.GrabJob())
            self.sent_timer = time()
        except Exception as e:
            logger.exception(e)

    def is_timeout(self):
        return self.sent_timer + 300 * 60 < time()


class Worker(BaseClient):
    def __init__(self, enabled_tasks=[]):
        BaseClient.__init__(self, TYPE_WORKER, self._message_callback,
                            self._on_connected)

        self.defrsps = {}
        self.lockers = {}
        self._tasks = {}
        self._broadcast_tasks = []
        self.enabled_tasks = enabled_tasks
        self._pool = None

        self.grab_agents = {}

    def set_enable_tasks(self, enabled_tasks):
        self.enabled_tasks = enabled_tasks

    def is_enabled(self, func):
        if len(self.enabled_tasks) == 0:
            return True
        return func in self.enabled_tasks

    async def _on_connected(self):
        for func in self._tasks.keys():
            if func in self._broadcast_tasks:
                await self._broadcast(func)
            else:
                await self._add_func(func)

        await asyncio.sleep(1)

    async def _add_func(self, func):
        if not self.is_enabled(func):
            return
        logger.info(f'Add {func}')
        agent = self.agent
        await agent.send(cmd.CanDo(self._add_prefix_subfix(func)))
        self.remove_agent(agent)

    async def add_func(self, func, task, defrsp=DoneResponse(), locker=None):
        if self.connected:
            await self._add_func(func)

        self._tasks[func] = task
        self.defrsps[func] = defrsp
        self.lockers[func] = locker

    async def _broadcast(self, func):
        if not self.is_enabled(func):
            return
        logger.info(f'Broadcast {func}')
        agent = self.agent
        await agent.send(cmd.Broadcast(self._add_prefix_subfix(func)))
        self.remove_agent(agent)

    async def broadcast(self, func, task, defrsp=DoneResponse(), locker=None):
        if self.connected:
            await self._broadcast(func)

        self._tasks[func] = task
        self.defrsps[func] = defrsp
        self.lockers[func] = locker
        self._broadcast_tasks.append(func)

    async def remove_func(self, func):
        logger.info(f'Remove {func}')
        agent = self.agent
        await agent.send(cmd.CantDo(self._add_prefix_subfix(func)))
        self.remove_agent(agent)
        self._tasks.pop(func, None)
        self.defrsps.pop(func, None)
        self.lockers.pop(func, None)
        if func in self._broadcast_tasks:
            self._broadcast_tasks.remove(func)

    async def next_grab(self):
        for agent in self.grab_agents.values():
            if agent.is_timeout():
                await agent.safe_send()
                break

    async def work(self, size, min_delay=60):
        self._pool = AioPool(size=size)
        agents = [self.agent for _ in range(size)]
        for agent in agents:
            self.grab_agents[agent.msgid] = GrabAgent(agent)

        delay = max(min_delay, int(300 / size))

        while True:
            await self.next_grab()
            await asyncio.sleep(delay)

    async def _message_callback(self, payload, msgid):
        await self.next_grab()
        self._pool.spawn_n(self.run_task(payload, msgid))

    async def run_task(self, payload, msgid):
        try:
            job = Job(payload[1:], self)
            await self.process_job(job)
        except asyncio.exceptions.CancelledError:
            pass
        finally:
            agent = self.grab_agents[msgid]
            await agent.safe_send()

    async def process_job(self, job):
        task = self._tasks.get(job.func_name)
        if not task:
            await self.remove_func(job.func_name)
            await job.fail()
        else:

            async def process():
                await self._process_job(job, task)

            locker = self.lockers.get(job.func_name)
            if locker:
                locker_name, count = locker(job)
                await job.with_lock(locker_name, count, process)
            else:
                await process()

    async def _process_job(self, job, task):
            try:
                if asyncio.iscoroutinefunction(task):
                    ret = await task(job)
                else:
                    ret = task(job)

            except Exception as e:
                logger.exception(e)
                ret = self.defrsps.get(job.func_name, FailResponse())

            if not job.finished:
                if isinstance(ret, str):
                    await job.done(bytes(ret, 'utf-8'))
                elif isinstance(ret, bytes):
                    await job.done(ret)
                elif isinstance(ret, DoneResponse):
                    await job.done(ret.buf)
                elif isinstance(ret, FailResponse):
                    await job.fail()
                elif isinstance(ret, SchedLaterResponse):
                    await job.sched_later(ret.delay, ret.count)
                else:
                    await job.done()

    # decorator
    def func(self, func_name, broadcast=False, defrsp=DoneResponse(), locker=None):
        def _func(task):
            self._tasks[func_name] = task
            self.defrsps[func_name] = defrsp
            self.lockers[func_name] = locker
            if broadcast:
                self._broadcast_tasks.append(func_name)
            return task

        return _func

    def blueprint(self, app):
        app.set_worker(self)
        self._tasks.update(app.tasks)
        self.defrsps.update(app.defrsps)
        self.lockers.update(app.lockers)

        for btsk in app.broadcast_tasks:
            self._broadcast_tasks.append(btsk)


class WorkerCluster(BaseCluster):
    def __init__(self, *args, **kwargs):
        BaseCluster.__init__(self, Worker, *args, **kwargs)

    def set_enable_tasks(self, enabled_tasks):
        self.run_sync('set_enable_tasks', enabled_tasks)

    def is_enabled(self, func):
        def reduce(acc, a):
            return acc and a

        return self.run_sync('is_enabled',
                             func,
                             reduce=reduce,
                             initialize=True)

    async def add_func(self, func, task, defrsp=DoneResponse(), locker=None):
        await self.run('add_func', func, task, defrsp, locker)

    async def broadcast(self, func, task, defrsp=DoneResponse(), locker=None):
        await self.run('broadcast', func, task, defrsp, locker)

    async def remove_func(self, func):
        await self.run('remove_func', func)

    async def work(self, size):
        await self.run('work', size)

    # decorator
    def func(self, func_name, broadcast=False, defrsp=DoneResponse(), locker=None):
        def _func(task):
            def reduce(_, call):
                call(task)

            self.run_sync('func', func_name, broadcast, defrsp, locker, reduce=reduce)

            return task

        return _func

    def blueprint(self, app):
        self.run_sync('blueprint', app)
