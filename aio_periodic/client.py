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

    def removeJob(self, job):
        agent = self.agent
        yield from agent.send([utils.REMOVE_JOB, json.dumps(job)])
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
        payload = str(payload, 'utf-8').strip()
        stats = payload.split('\n')
        retval = {}
        for stat in stats:
            stat = stat.split(",")
            retval[stat[0]] = {
                'func_name': stat[0],
                'worker_count': int(stat[1]),
                'job_count': int(stat[2]),
                'processing': int(stat[3])
            }

        return retval

    def dropFunc(self, func):
        agent = self.agent
        yield from agent.send([utils.DROP_FUNC, func])
        payload = yield from agent.recive()
        self.remove_agent(agent)
        if payload == utils.SUCCESS:
            return True
        else:
            return False
