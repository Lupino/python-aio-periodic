from .job import Job
from .types.utils import TYPE_WORKER
from .types.base_client import BaseClient
from .types import command as cmd
import asyncio


class Worker(BaseClient):

    def __init__(self, loop=None):
        BaseClient.__init__(self, TYPE_WORKER, loop)
        self._tasks = {}

    async def add_func(self, func, task):
        agent = self.agent
        await agent.send(cmd.CanDo(func))
        self.remove_agent(agent)
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

    async def _work(self):
        agent = self.agent
        loop = self.loop
        timer = None
        def __work():
            loop.create_task(send_grab_job())

        async def send_grab_job():
            if agent.buffer_len() > 0:
                payload = await agent.recive()
                if payload[0:1] == cmd.NO_JOB and payload[0:1] != cmd.JOB_ASSIGN:
                    timer = loop.call_later(10, __work)
                else:
                    try:
                        await process_job(Job(payload[1:], self, agent))
                    except Exception as e:
                        print(e)
                        timer = loop.call_soon(1, __work)
            else:
                await agent.send(cmd.GrabJob())
                timer = loop.call_later(1, __work)

        async def process_job(job):
            task = self._tasks.get(job.func_name)
            if not task:
                await self.remove_func(job.func_name)
                await job.fail()
            else:
                try:
                    await task(job)
                except Exception as e:
                    print(e)
                    await job.fail()

            loop.call_soon(__work)

        loop.call_soon(__work)

    def work(self, size):
        for _ in range(size):
            self.loop.create_task(self._work())
