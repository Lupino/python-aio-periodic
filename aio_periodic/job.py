from .types import utils
from .types.job import Job as J
from .types import command as cmd
import json


class FinishedError(Exception):
    pass


class Job(object):
    def __init__(self, payload, w):

        self.payload = J.build(payload)

        self.job_handle = utils.encode_str8(
            self.payload.func) + utils.encode_str8(self.payload.name)

        self.w = w

        self.finished = False

    def _check_finished(self):
        if self.finished:
            raise FinishedError('Job is already finished')

        self.finished = True

    async def done(self, buf=b''):
        self._check_finished()
        agent = await self.w.gen_agent()
        await agent.send(cmd.WorkDone(self.job_handle, buf))

    async def sched_later(self, delay, count=0):
        self._check_finished()
        agent = await self.w.gen_agent()
        await agent.send(cmd.SchedLater(self.job_handle, delay, count))

    async def fail(self):
        self._check_finished()
        agent = await self.w.gen_agent()
        await agent.send(cmd.WorkFail(self.job_handle))

    async def acquire(self, name, count):
        agent = await self.w.gen_agent()
        await agent.send(cmd.Acquire(name, count, self.job_handle))
        payload = await agent.receive()
        if payload[0:1] == cmd.ACQUIRED:
            if payload[1] == 1:
                return True

        self.finished = True
        return False

    async def release(self, name):
        agent = await self.w.gen_agent()
        await agent.send(cmd.Release(name, self.job_handle))

    async def with_lock(self, name, count, task, release=False):
        acquired = await self.acquire(name, count)
        if acquired:
            await task()
            if release:
                await self.release(name)

    @property
    def func_name(self):
        return self.w._strip_prefix_subfix(str(self.payload.func, 'utf-8'))

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

    @property
    def workload_json(self):
        return json.loads(str(self.payload.workload, 'utf-8'))
