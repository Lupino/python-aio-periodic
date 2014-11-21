from .job import Job
from . import utils
from .utils import BaseClient, TYPE_WORKER


class Worker(BaseClient):

    def __init__(self):
        BaseClient.__init__(self, TYPE_WORKER)

    def grabJob(self):
        agent = self.agent
        yield from agent.send([utils.GRAB_JOB])
        payload = yield from agent.recive()
        cmd = payload[0]
        if payload[0] == utils.NO_JOB[0] and cmd != utils.JOB_ASSIGN[0]:
            self.remove_agent(agent)
            return None

        payload = payload[3:]
        return Job(payload, self, agent)

    def add_func(self, func):
        agent = self.agent
        yield from agent.send([utils.CAN_DO, func])
        self.remove_agent(agent)

    def remove_func(self, func):
        agent = self.agent
        yield from agent.send([utils.CANT_DO, func])
        self.remove_agent(agent)
