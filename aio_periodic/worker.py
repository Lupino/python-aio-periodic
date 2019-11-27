from .job import Job
from .types.utils import TYPE_WORKER
from .types.base_client import BaseClient
from .types import command as cmd
import asyncio

import logging
logger = logging.getLogger('aio_periodic.worker')


class Worker(BaseClient):

    def __init__(self, loop=None):
        BaseClient.__init__(self, TYPE_WORKER, loop)
        self._tasks = {}

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

    def _work(self):
        agent = self.agent
        loop = self.loop

        class Work(object):
            def __init__(w):
                w.timer = None

            def run(w, delay=0):
                if w.timer:
                    w.timer.cancel()
                    w.timer = None

                if delay > 0:
                    timer = loop.call_later(delay, w.run, 0)
                else:
                    loop.call_soon(loop.create_task, w.send_grab_job())

            async def send_grab_job(w):
                if agent.buffer_len() > 0:
                    payload = await agent.recive()
                    if payload[0:1] == cmd.NO_JOB and payload[0:1] != cmd.JOB_ASSIGN:
                        w.run(10)
                    else:
                        job = None
                        try:
                            job = Job(payload[1:], self)
                        except Exception as e:
                            print('decode job failed', e, payload)

                        if job:
                            await process_job(job)

                        w.run()
                else:
                    if self.connected == True:
                        await agent.send(cmd.GrabJob())
                        w.run(1)

        async def process_job(job):
            task = self._tasks.get(job.func_name)
            if not task:
                await self.remove_func(job.func_name)
                await job.fail()
            else:
                try:
                    await task(job)
                except Exception as e:
                    print('process_job failed', e)
                    await job.fail()

        Work().run()

    async def work(self, size):
        while True:
            for _ in range(size):
                self._work()


            while True:
                waiter = self.add_lose_waiter()
                await waiter
                try:
                    for func in self._tasks.keys():
                        await self.add_func(func)
                    break
                except Exception as e:
                    logger.exception(e)
                    await asyncio.sleep(2)
