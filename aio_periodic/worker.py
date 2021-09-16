from .job import Job
from .types.utils import TYPE_WORKER
from .types.base_client import BaseClient, BaseCluster
from .types.agent import Agent
from .types import command as cmd
import asyncio
from asyncio_pool import AioPool
import random

import logging
logger = logging.getLogger(__name__)


class Worker(BaseClient):
    def __init__(self, enabled_tasks=[], loop=None):
        BaseClient.__init__(self, TYPE_WORKER, loop, self._message_callback,
                            self._on_connected)
        self._tasks = {}
        self.enabled_tasks = enabled_tasks
        self._pool = None

        self.prefix = None
        self.subfix = None

    def set_prefix(self, prefix):
        self.prefix = prefix

    def set_subfix(self, subfix):
        self.subfix = subfix

    def _add_prefix_subfix(self, func):
        if self.prefix:
            func = '{}{}'.format(self.prefix, func)

        if self.subfix:
            func = '{}{}'.format(func, self.subfix)

        return func

    def _strip_prefix_subfix(self, func):
        if self.prefix and func.startswith(self.prefix):
            func = func[len(self.prefix):]
        if self.subfix and func.endswith(self.subfix):
            func = func[:-len(self.subfix)]
        return func

    def set_enable_tasks(self, enabled_tasks):
        self.enabled_tasks = enabled_tasks

    def is_enabled(self, func):
        if len(self.enabled_tasks) == 0:
            return True
        return func in self.enabled_tasks

    async def _on_connected(self):
        for func in self._tasks.keys():
            await self._add_func(func)

        await asyncio.sleep(1)


    async def _add_func(self, func):
        if not self.is_enabled(func):
            return
        logger.info('Add {}'.format(func))
        agent = self.agent
        await agent.send(cmd.CanDo(self._add_prefix_subfix(func)))
        self.remove_agent(agent)

    async def add_func(self, func, task=None):
        if self.connected:
            await self._add_func(func)
        if task:
            self._tasks[func] = task

    async def broadcast(self, func, task):
        logger.info('Broadcast {}'.format(func))
        agent = self.agent
        await agent.send(cmd.Broadcast(self._add_prefix_subfix(func)))
        self.remove_agent(agent)
        self._tasks[func] = task

    async def remove_func(self, func):
        logger.info('Remove {}'.format(func))
        agent = self.agent
        await agent.send(cmd.CantDo(self._add_prefix_subfix(func)))
        self.remove_agent(agent)
        self._tasks.pop(func, None)

    async def work(self, size):
        self._pool = AioPool(size=size)
        agents = [self.agent for _ in range(size)]

        async def safe_send(agent):
            if not self.connected:
                waiter = await self.make_waiter()
                await waiter

            try:
                await agent.send(cmd.GrabJob())
            except Exception as e:
                logger.exception(e)

        for agent in agents:
            await safe_send(agent)

        while True:
            if self._pool.is_empty:
                agent = random.choice(agents)
                await safe_send(agent)
                await asyncio.sleep(120)
                continue

            if self._pool.is_full:
                await asyncio.sleep(1)
                continue

            choices = random.choices(agents, k = size - len(self._pool))
            for agent in choices:
                await safe_send(agent)

            await asyncio.sleep(60)

    async def _message_callback(self, payload, msgid):
        await self._pool.spawn(self.run_task(payload, msgid))

    async def run_task(self, payload, msgid):
        try:
            job = Job(payload[1:], self)
            await self.process_job(job)
        finally:
            agent = Agent(self, msgid, self.loop)
            await agent.send(cmd.GrabJob())

    async def process_job(self, job):
        task = self._tasks.get(job.func_name)
        if not task:
            await self.remove_func(job.func_name)
            await job.fail()
        else:
            try:
                await task(job)
            except Exception as e:
                logger.exception(e)
                await job.fail()

    # decorator
    def func(self, func_name):
        def _func(task):
            self._tasks[func_name] = task
            return task

        return _func


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

    async def add_func(self, func, task=None):
        await self.run('add_func', func, task)

    async def broadcast(self, func, task):
        await self.run('broadcast', func, task)

    async def remove_func(self, func):
        await self.run('remove_func', func)

    async def work(self, size):
        await self.run('work', size)

    # decorator
    def func(self, func_name):
        def _func(task):
            def reduce(_, call):
                call(task)

            self.run_sync('func', func_name, reduce=reduce)

            return task

        return _func
