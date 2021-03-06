import asyncio
from .agent import Agent
from .utils import decode_int32, MAGIC_RESPONSE
import uuid
from .command import PING, PONG, NO_JOB, JOB_ASSIGN
from . import command as cmd
from binascii import crc32
from .job import Job
from time import time

try:
    from uhashring import HashRing
except Exception:
    HashRing = None

import logging

logger = logging.getLogger('aio_periodic.types.base_client')


class BaseClient(object):
    def __init__(self,
                 clientType,
                 loop=None,
                 message_callback=None,
                 on_connected=None):
        self.connected = False
        self.connid = None
        self._reader = None
        self._writer = None
        self._buffer = b''
        self._clientType = clientType
        self.agents = dict()

        self.loop = loop

        self._on_connected = on_connected

        self.disconnecting_waiters = []

        self._connector = None
        self._connector_args = None
        self._cb = message_callback
        self._initialized = False
        self._send_locker = asyncio.Lock()
        self._receive_timer = 0
        self._send_timer = 0

    def initialize(self, loop=None):
        self._initialized = True

        if loop is not None:
            self.loop = loop
        if self.loop is None:
            self.loop = asyncio.get_event_loop()

        self.loop_agent_waiter = self.loop.create_future()

        self.loop.create_task(self.loop_agent())
        self.loop.create_task(self.check_alive())
        self.loop.create_task(self.monitor())

    async def connect(self, connector=None, *args, loop=None):
        if not self._initialized:
            self.initialize(loop)

        if connector:
            self._connector = connector
            self._connector_args = args

        reader, writer = await self._connector(*self._connector_args)
        if self._writer:
            try:
                self._writer.close()
            except Exception as e:
                logger.exception(e)

        self._writer = writer
        self._reader = reader
        self._buffer = b''
        agent = Agent(self, None, self.loop)
        await agent.send(self._clientType)
        self.connected = True
        if self._on_connected:
            await self._on_connected()

        if self.loop_agent_waiter:
            try:
                self.loop_agent_waiter.set_result(True)
            except Exception as e:
                logger.exception(e)

        return True

    async def _receive(self, size):
        while True:
            if len(self._buffer) >= size:
                buf = self._buffer[:size]
                self._buffer = self._buffer[size:]
                return buf

            buf = await self._reader.read(max(4096, size))
            if len(buf) == 0:
                break

            self._buffer += buf

    async def check_alive(self):
        while True:
            if self.connected:
                try:
                    await self.ping()
                except Exception as e:
                    logger.exception(e)
                    self.connected = False
            await asyncio.sleep(1)

    async def monitor(self):
        while True:
            now = time()
            if self._send_timer + 300 < now:
                self.connected = False

            if self._receive_timer > self._send_timer:
                delay = self._receive_timer - self._send_timer
                if delay > 600:
                    self.connected = False

            await asyncio.sleep(1)

    @property
    def agent(self):
        msgid = bytes(uuid.uuid4().hex[:4], 'utf-8')
        while self.agents.get(msgid):
            msgid = bytes(uuid.uuid4().hex[:4], 'utf-8')

        agent = Agent(self, msgid, self.loop)
        self.agents[msgid] = agent
        return agent

    def get_writer(self):
        return self._writer

    async def loop_agent(self):
        async def receive():
            magic = await self._receive(4)
            if not magic:
                self.close()
                raise Exception("Closed")
            if magic != MAGIC_RESPONSE:
                self.close()
                raise Exception('Magic not match.')
            header = await self._receive(4)
            length = decode_int32(header)
            crc = await self._receive(4)
            payload = await self._receive(length)
            if decode_int32(crc) != crc32(payload):
                raise Exception('CRC not match.')
            return payload

        async def main_receive_loop():
            while self.connected:
                payload = await receive()
                self._receive_timer = time()
                msgid = payload[0:4]
                agent = self.agents.get(msgid)
                payload = payload[4:]

                if payload[0:1] == NO_JOB:
                    continue
                if payload[0:1] == JOB_ASSIGN:
                    if self._cb:
                        await self._cb(payload, msgid)
                    continue

                if agent:
                    agent.feed_data(payload)
                else:
                    logger.error('Agent %s not found.' % msgid)

        while True:
            if self.loop_agent_waiter:
                await self.loop_agent_waiter
                self.loop_agent_waiter = None

            try:
                if self.connected:
                    self.connid = await receive()
                    await main_receive_loop()
            except Exception as e:
                logger.exception(e)
                self.connected = False

            self.loop_agent_waiter = self.loop.create_future()
            delay = 0
            while True:
                try:
                    await self.connect()
                    break
                except Exception as e:
                    logger.exception(e)

                delay += 2

                if delay > 30:
                    delay = 30

                await asyncio.sleep(delay)

            waiters = self.disconnecting_waiters[:]
            self.disconnecting_waiters = []
            for waiter in waiters:
                waiter.set_result(True)

    async def ping(self, timeout=10):
        agent = self.agent
        await agent.send(PING)
        ret = self.loop.create_future()

        async def receive():
            payload = await agent.receive()
            self.agents.pop(agent.msgid)
            if payload == PONG:
                try:
                    ret.set_result(True)
                except Exception:
                    pass
            else:
                try:
                    ret.set_result(False)
                except Exception:
                    pass

        async def timecheck():
            await asyncio.sleep(timeout)
            try:
                ret.set_result(False)
            except Exception:
                pass

        task1 = self.loop.create_task(receive())
        task2 = self.loop.create_task(timecheck())

        r = await ret

        task1.cancel()
        task2.cancel()

        return r

    def add_lose_waiter(self):
        waiter = self.loop.create_future()
        self.disconnecting_waiters.append(waiter)
        return waiter

    def remove_agent(self, agent):
        self.agents.pop(agent.msgid, None)

    def close(self):
        if self._writer:
            self._writer.close()

    async def submit_job(self, *args, job=None, **kwargs):
        agent = self.agent
        if job is None:
            job = Job(*args, **kwargs)
        await agent.send(cmd.SubmitJob(job))
        payload = await agent.receive()
        self.remove_agent(agent)
        if payload == cmd.SUCCESS:
            return True
        else:
            return False

    async def run_job(self, *args, job=None, **kwargs):
        agent = self.agent
        if job is None:
            job = Job(*args, **kwargs)
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
                'locked': int(stat[4]),
                'sched_at': int(stat[5])
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


class BaseCluster(object):
    def __init__(self, clientclass, entrypoints, *args, loop=None, **kwargs):
        self.clients = []
        nodes = {}

        for entrypoint in entrypoints:
            client = clientclass(*args, loop=loop, **kwargs)
            self.clients.append(client)
            nodes[entrypoint] = {
                'hostname': entrypoint,
                'instance': client
            }

        if HashRing is None:
            raise Exception('Please install uhashring library.')

        self.entrypoints = entrypoints
        self.hr = HashRing(nodes=nodes, hash_fn='ketama')

    def get(self, name):
        '''get one client by hashring'''
        return self.hr[name]

    async def run(self,
                  method_name,
                  *args,
                  reduce=None,
                  initialize=None,
                  **kwargs):
        retval = initialize
        for client in self.clients:
            method = getattr(client, method_name)
            ret = await method(*args, **kwargs)
            if reduce:
                retval = reduce(retval, ret)

        return retval

    def run_sync(self,
                 method_name,
                 *args,
                 reduce=None,
                 initialize=None,
                 **kwargs):
        retval = initialize
        for client in self.clients:
            method = getattr(client, method_name)
            ret = method(*args, **kwargs)
            if reduce:
                retval = reduce(retval, ret)

        return retval

    async def connect(self, connector=None, *args, loop=None):
        '''connect to servers'''
        for entrypoint, client in zip(self.entrypoints, self.clients):
            await client.connect(connector, entrypoint, *args, loop=loop)

    def close(self):
        '''close all the servers'''
        self.run_sync('close')

    async def submit_job(self, *args, job=None, **kwargs):
        '''submit job to one server'''
        if job is None:
            job = Job(*args, **kwargs)
        client = self.get(job.name)
        return await client.submit_job(job=job)

    async def run_job(self, *args, job=None, **kwargs):
        '''run job to one server'''
        if job is None:
            job = Job(*args, **kwargs)
        client = self.get(job.name)
        return await client.run_job(job=job)

    async def remove_job(self, func, name):
        '''remove job from servers'''
        client = self.get(name)
        return await client.remove_job(func, name)

    async def drop_func(self, func):
        '''drop func from servers'''
        await self.run('drop_func', func)

    async def status(self):
        '''status from servers and merge the result'''
        def reduce(stats, stat):
            for func in stat.keys():
                if not stats.get(func):
                    stats[func] = stat[func]
                else:
                    stats[func]['worker_count'] += stat[func]['worker_count']
                    stats[func]['job_count'] += stat[func]['job_count']
                    stats[func]['processing'] += stat[func]['processing']

                    if stats[func]['sched_at'] > stat[func]['sched_at']:
                        stats[func]['sched_at'] = stat[func]['sched_at']

            return stats

        return await self.run('status', reduce=reduce, initialize={})
