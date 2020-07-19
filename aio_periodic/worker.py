from .job import Job
from .types.utils import TYPE_WORKER
from .types.base_client import BaseClient
from .types import command as cmd
import asyncio

import logging
logger = logging.getLogger('aio_periodic.worker')


class Worker(BaseClient):

    def __init__(self, loop=None):
        BaseClient.__init__(self, TYPE_WORKER, loop, self._message_callback)
        self._tasks = {}
        self._locker = asyncio.Lock()
        self._waiters = {}

    async def add_func(self, func, task = None):
        agent = self.agent
        await agent.send(cmd.CanDo(func))
        self.remove_agent(agent)
        if task:
            self._tasks[func] = task

    async def broadcast(self, func, task):
        agent = self.agent
        await agent.send(cmd.Broadcast(func))
        self.remove_agent(agent)
        self._tasks[func] = task

    async def remove_func(self, func):
        agent = self.agent
        await agent.send(cmd.CantDo(func))
        self.remove_agent(agent)
        self._tasks.pop(func, None)

    async def work(self, size):
        for _ in range(size):
            self.loop.create_task(self._work())

    async def _work(self):
        agent = self.agent
        while True:
            await asyncio.sleep(1)

            async with self._locker:
                waiter = self._waiters.pop(agent.msgid, None)

            if waiter:
                await waiter

            await agent.send(cmd.GrabJob())



    async def _message_callback(self, payload, msgid):
        self.loop.create_task(self.run_task(payload, msgid))


    async def run_task(self, payload, msgid):
        waiter = self.loop.create_future()
        async with self._locker:
            self._waiters[msgid] = waiter

        try:
            job = Job(payload[1:], self)
            await self.process_job(job)
        finally:
            waiter.set_result(True)


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
