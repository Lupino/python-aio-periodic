from . import utils

class Job(object):

    def __init__(self, payload, w, agent):
        payload = payload.split(utils.NULL_CHAR, 2)
        self.payload = utils.decodeJob(payload[2])
        self.job_handle = utils.to_str(payload[1])

        self.agent = agent
        self._worker = w

    def get(self, key, default=None):
        return self.payload.get(key, default)

    def done(self):
        yield from self.agent.send([utils.WORK_DONE, self.job_handle])
        self._worker.remove_agent(self.agent)

    def sched_later(self, delay):
        yield from self.agent.send(
            [utils.SCHED_LATER, self.job_handle, str(delay)])
        self._worker.remove_agent(self.agent)

    def fail(self):
        yield from self.agent.send([utils.WORK_FAIL, self.job_handle])
        self._worker.remove_agent(self.agent)

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
