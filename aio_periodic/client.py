from .utils import BaseClient, TYPE_CLIENT
from . import utils
import json

class Client(BaseClient):

    def __init__(self):
        BaseClient.__init__(self, TYPE_CLIENT)

    def submitJob(self, job):
        agent = self.agent
        yield from agent.send([utils.SUBMIT_JOB, json.dumps(job)])
        payload = yield from agent.recive()
        self.remove_agent(agent)
        if payload == utils.SUCCESS:
            return True
        else:
            return False

    def status(self):
        agent = self.agent
        yield from agent.send([utils.STATUS])
        payload = yield from agent.recive()
        self.remove_agent(agent)
        return json.loads(str(payload, "utf-8"))

    def dropFunc(self, func):
        agent = self.agent
        yield from agent.send([utils.DROP_FUNC, func])
        payload = yield from agent.recive()
        self.remove_agent(agent)
        if payload == utils.SUCCESS:
            return True
        else:
            return False
