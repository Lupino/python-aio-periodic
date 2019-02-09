from .types import utils
from .types.job import Job as J
from .types import command as cmd

class Job(object):

    def __init__(self, payload, w, agent):

        self.payload = J.build(payload)

        self.job_handle = utils.encode_str8(self.payload.func) + utils.encode_str8(self.payload.name)

        self.agent = agent
        self._worker = w

    async def done(self, buf = b''):
        await self.agent.send(cmd.WorkDone(self.job_handle, buf))

    async def sched_later(self, delay, count = 0):
        await self.agent.send(cmd.SchedLater(self.job_handle, delay, count))

    async def fail(self):
        await self.agent.send(cmd.WorkFail(self.job_handle))

    @property
    def func_name(self):
        return str(self.payload.func, 'utf-8')

    @property
    def name(self):
        return str(self.payload.name, 'utf-8')

    @property
    def sched_at(self):
        return self.payload.sched_at

    @property
    def timeout(self):
        return self.payload.timeout

    @property
    def workload(self):
        return self.payload.workload
