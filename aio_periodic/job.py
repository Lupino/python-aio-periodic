import json
from . import utils


class Job(object):

    def __init__(self, payload, client):
        payload = payload.split(utils.NULL_CHAR)
        self.payload = json.loads(str(payload[2], "UTF-8"))
        self.job_handle = str(payload[1], "UTF-8")
        self.client = client
        self.msgId = payload[0]


    def get(self, key, default=None):
        return self.payload.get(key, default)


    def done(self):
        yield from self.client.send([self.msgId, utils.JOB_DONE, self.job_handle])


    def sched_later(self, delay):
        yield from self.client.send([self.msgId, utils.SCHED_LATER, self.job_handle, str(delay)])


    def fail(self):
        yield from self.client.send([self.msgId, utils.JOB_FAIL, self.job_handle])


    @property
    def func_name(self):
        return self.payload['func']


    @property
    def name(self):
        return self.payload.get("name")


    @property
    def sched_at(self):
        return self.payload["sched_at"]


    @property
    def timeout(self):
        return self.payload.get("timeout", 0)


    @property
    def run_at(self):
        return self.payload.get("run_at", self.sched_at)


    @property
    def workload(self):
        return self.payload.get("workload")
