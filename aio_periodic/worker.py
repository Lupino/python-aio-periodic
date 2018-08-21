from .job import Job
from .types.utils import TYPE_WORKER
from .types.base_client import BaseClient
from .types import command as cmd
import asyncio


class Worker(BaseClient):

    def __init__(self, loop=None):
        BaseClient.__init__(self, TYPE_WORKER, loop)
        self._tasks = {}

    def add_func(self, func, task):
        agent = self.agent
        yield from agent.send(cmd.CanDo(func))
        self.remove_agent(agent)
        self._tasks[func] = task

    def broadcast(self, func, task):
        agent = self.agent
        yield from agent.send(cmd.Broadcast(func))
        self.remove_agent(agent)
        self._tasks[func] = task

    def remove_func(self, func):
        agent = self.agent
        yield from agent.send(cmd.CantDo(func))
        self.remove_agent(agent)
        self._tasks.pop(func, None)

    def _work(self):
        agent = self.agent
        loop = self.loop
        timer = None
        def send_grab_job():
            if agent.buffer_len() > 0:
                payload = yield from agent.recive()
                if payload[0:1] == cmd.NO_JOB and payload[0:1] != cmd.JOB_ASSIGN:
                    timer = loop.call_later(10, send_grab_job)
                else:
                    process_job(Job(payload[1:], self, agent))
            else:
                yield from agent.send(cmd.GrabJob())
                timer = loop.call_later(1, send_grab_job)

        def process_job(job):
            task = self._tasks.get(job.func_name)
            if not task:
                yield from self.remove_func(job.func_name)
                yield from job.fail()
            else:
                try:
                    yield from task(job)
                except Exception as e:
                    print(e)
                    yield from job.fail()

            loop.call_soon(send_grab_job)

        loop.call_soon(send_grab_job)

    def work(self, size):
        for _ in range(size):
            self.loop.create_task(self._work)
