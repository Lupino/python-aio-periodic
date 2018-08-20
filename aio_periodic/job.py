from .types import utils
from .types.job import Job as J
from .types import command as cmd

class Job(object):

    def __init__(self, payload, w, agent):
        h = utils.decode_int8(payload[0:1])
        self.job_handle = payload[0:h + 1]

        payload = payload[h+1:]

        self.payload = J.build(payload)

        self.agent = agent
        self._worker = w

    def done(self, buf = b''):
        yield from self.agent.send(cmd.WorkDone(self.job_handle, buf))
        self._worker.remove_agent(self.agent)

    def sched_later(self, delay, count = 0):
        yield from self.agent.send(cmd.SchedLater(self.job_handle, delay, count))
        self._worker.remove_agent(self.agent)

    def fail(self):
        yield from self.agent.send(cmd.WorkFail(self.job_handle))
        self._worker.remove_agent(self.agent)

    @property
    def func_name(self):
        return self.payload.func

    @property
    def name(self):
        return self.payload.name

    @property
    def sched_at(self):
        return self.payload.sched_at

    @property
    def timeout(self):
        return self.payload.timeout

    @property
    def workload(self):
        return self.payload.workload
