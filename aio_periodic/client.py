from .types.utils import TYPE_CLIENT
from .types.base_client import BaseClient
from .types import command as cmd

class Client(BaseClient):

    def __init__(self, loop=None):
        BaseClient.__init__(self, TYPE_CLIENT, loop)

    async def submit_job(self, job):
        agent = self.agent
        await agent.send(cmd.SubmitJob(job))
        payload = await agent.receive()
        self.remove_agent(agent)
        if payload == cmd.SUCCESS:
            return True
        else:
            return False

    async def run_job(self, job):
        agent = self.agent
        await agent.send(cmd.RunJob(job))
        payload = await agent.receive()
        self.remove_agent(agent)
        if payload[0] == cmd.NO_WORKER[0]:
            raise Exception('no worker')

        if payload[0] == cmd.DATA[0]:
            return payload[1:]

        return payload

    async def remove_job(self, func, name):
        agent = self.agent
        await agent.send(cmd.RemoveJob(func, name))
        payload = await agent.receive()
        self.remove_agent(agent)
        if payload == cmd.SUCCESS:
            return True
        else:
            return False

    async def status(self):
        agent = self.agent
        await agent.send(cmd.Status())
        payload = await agent.receive()
        self.remove_agent(agent)
        payload = str(payload, 'utf-8').strip()
        stats = payload.split('\n')
        retval = {}
        for stat in stats:
            stat = stat.strip()
            if not stat:
                continue
            stat = stat.split(',')
            retval[stat[0]] = {
                'func_name': stat[0],
                'worker_count': int(stat[1]),
                'job_count': int(stat[2]),
                'processing': int(stat[3]),
                'sched_at': int(stat[4])
            }

        return retval

    async def drop_func(self, func):
        agent = self.agent
        await agent.send(cmd.DropFunc(func))
        payload = await agent.receive()
        self.remove_agent(agent)
        if payload == cmd.SUCCESS:
            return True
        else:
            return False
