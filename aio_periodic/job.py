from .types import utils
from .types.job import Job as J
from .types import command as cmd

class Job(object):

    def __init__(self, payload, w):

        self.payload = J.build(payload)

        self.job_handle = utils.encode_str8(self.payload.func) + utils.encode_str8(self.payload.name)

        self.w = w

    async def done(self, buf = b''):
        await self.w.agent.send(cmd.WorkDone(self.job_handle, buf))

    async def sched_later(self, delay, count = 0):
        await self.w.agent.send(cmd.SchedLater(self.job_handle, delay, count))

    async def fail(self):
        await self.w.agent.send(cmd.WorkFail(self.job_handle))

    async def acquire(self, name, count):
        agent = self.w.agent
        await agent.send(cmd.Acquire(name, count, self.job_handle))
        payload = await agent.recive()
        if payload[0:1] == cmd.ACQUIRED:
            if payload[1] == 1:
                return True
        return False

    async def release(self, name):
        await self.w.agent.send(cmd.Release(name, self.job_handle))

    async def with_lock(self, name, count, task):
        acquired = await self.acquire(name, count)
        if acquired:
            await task()
            await self.release(name)

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
