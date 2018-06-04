from .job import Job
from . import utils
from .utils import BaseClient, TYPE_WORKER


class Worker(BaseClient):

    def __init__(self):
        BaseClient.__init__(self, TYPE_WORKER)

    def grab_job(self):
        agent = self.agent
        yield from agent.send(utils.GRAB_JOB)
        payload = yield from agent.recive()
        if payload[0:1] == utils.NO_JOB and payload[0:1] != utils.JOB_ASSIGN:
            self.remove_agent(agent)
            return None

        return Job(payload[1:], self, agent)

    def add_func(self, func):
        agent = self.agent
        yield from agent.send([utils.CAN_DO, utils.encode_str8(func)])
        self.remove_agent(agent)

    def broadcast(self, func):
        agent = self.agent
        yield from agent.send([utils.BROADCAST, utils.encode_str8(func)])
        self.remove_agent(agent)

    def remove_func(self, func):
        agent = self.agent
        yield from agent.send([utils.CANT_DO, utils.encode_str8(func)])
        self.remove_agent(agent)
