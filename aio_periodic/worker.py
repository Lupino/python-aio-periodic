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
        self._sem = None
        self._task_size = 0
        self._locker = asyncio.Lock()

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

    async def _send_grab(self):
        agent = self.agent
        await agent.send(cmd.GrabJob())
        self.remove_agent(agent)


    async def work(self, size):
        self._sem = asyncio.Semaphore(size)
        agent = self.agent
        await agent.send(cmd.GrabJob())

        while True:
            await asyncio.sleep(10)
            if self._sem.locked():
                continue

            if self._task_size < size:
                await agent.send(cmd.GrabJob())


    async def _message_callback(self, payload):
        async with lock:
            self._task_size += 1
        self.loop.create_task(self.run_task(payload))


    async def run_task(self, payload):
        await self._sem.acquire()
        try:
            if self._task_size < size:
                await self._send_grab()
            job = Job(payload[1:], self)
            await self.process_job(job)
        finally:
            self._sem.release()
            async with lock:
                self._task_size -= 1

            await self._send_grab()


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
