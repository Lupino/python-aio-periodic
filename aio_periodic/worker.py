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
        if payload == utils.NO_JOB or payload == utils.WAIT_JOB:
            self.remove_agent(agent)
            return None

        return Job(payload, self, agent)

    def add_func(self, func):
        agent = self.agent
        yield from agent.send([utils.CAN_DO, func])
        self.remove_agent(agent)

    def remove_func(self, func):
        agent = self.agent
        yield from agent.send([utils.CANT_DO, func])
        self.remove_agent(agent)
