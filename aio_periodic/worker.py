from .job import Job
from .types.utils import TYPE_WORKER
from .types.base_client import BaseClient
from .types import command as cmd


class Worker(BaseClient):

    def __init__(self):
        BaseClient.__init__(self, TYPE_WORKER)

    def grab_job(self):
        agent = self.agent
        yield from agent.send(cmd.GrabJob())
        payload = yield from agent.recive()
        if payload[0:1] == cmd.NO_JOB and payload[0:1] != cmd.JOB_ASSIGN:
            self.remove_agent(agent)
            return None

        return Job(payload[1:], self, agent)

    def add_func(self, func):
        agent = self.agent
        yield from agent.send(cmd.CanDo(func))
        self.remove_agent(agent)

    def broadcast(self, func):
        agent = self.agent
        yield from agent.send(cmd.Broadcast(func))
        self.remove_agent(agent)

    def remove_func(self, func):
        agent = self.agent
        yield from agent.send(cmd.CantDo(func))
        self.remove_agent(agent)
