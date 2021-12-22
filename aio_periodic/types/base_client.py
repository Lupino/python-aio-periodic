import asyncio
from .agent import Agent
from .utils import decode_int32, MAGIC_RESPONSE, encode_int32
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

logger = logging.getLogger(__name__)


class BaseClient(object):
    def __init__(self, clientType, message_callback=None, on_connected=None):

        self.connected_evt = None

        self.connid = None
        self._reader = None
        self._writer = None
        self._buffer = b''
        self._clientType = clientType
        self.agents = dict()
        self._msgid_locker = None
        self._last_msgid = 0

        self._on_connected = on_connected

        self._connector = None
        self._connector_args = None
        self._cb = message_callback
        self._initialized = False
        self._send_locker = None
        self._receive_timer = 0
        self._send_timer = 0
        self._processes = []

        self.prefix = None
        self.subfix = None

    def set_prefix(self, prefix):
        self.prefix = prefix

    def set_subfix(self, subfix):
        self.subfix = subfix

    def _add_prefix_subfix(self, func):
        if self.prefix:
            func = f'{self.prefix}{func}'

        if self.subfix:
            func = f'{func}{self.subfix}'

        return func

    def _strip_prefix_subfix(self, func):
        if self.prefix and func.startswith(self.prefix):
            func = func[len(self.prefix):]
        if self.subfix and func.endswith(self.subfix):
            func = func[:-len(self.subfix)]
        return func

    def initialize(self):
        self._initialized = True
        self.connected_evt = asyncio.Event()
        self._send_locker = asyncio.Lock()
        self._msgid_locker = asyncio.Lock()

        task = asyncio.create_task(self.loop_agent())
        self._processes.append(task)
        task = asyncio.create_task(self.check_alive())
        self._processes.append(task)
        task = asyncio.create_task(self.monitor())
        self._processes.append(task)

    async def connect(self, connector=None, *args):
        if not self._initialized:
            self.initialize()

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
        agent = Agent(self, None)
        await agent.send(self._clientType, True)
        self.connected_evt.set()
        if self._on_connected:
            await self._on_connected()

        return True

    @property
    def connected(self):
        return self.connected_evt.is_set()

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
            await self.connected_evt.wait()
            try:
                await self.ping()
            except Exception as e:
                logger.exception(e)
                self.connected_evt.clear()
            await asyncio.sleep(1)

    async def monitor(self):
        while True:
            now = time()
            if self._send_timer + 300 < now:
                self.connected_evt.clear()

            if self._receive_timer > self._send_timer:
                delay = self._receive_timer - self._send_timer
                if delay > 600:
                    self.connected_evt.clear()

            await asyncio.sleep(1)

    def get_next_msgid(self):
        for _ in range(1000000):
            self._last_msgid += 1
            if self._last_msgid > 0xFFFFFF00:
                self._last_msgid = 0

            if self._last_msgid % 100000 == 0:
                count = len(self.agents.keys())
                logger.info(f'Last msgid {self._last_msgid} Agents {count}')

            msgid = encode_int32(self._last_msgid)
            if not self.agents.get(msgid):
                return msgid

        raise Exception('Not enough msgid avaliable')

    async def gen_agent(self):
        async with self._msgid_locker:
            msgid = self.get_next_msgid()

        agent = Agent(self, msgid)
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
            await self.connected_evt.wait()

            try:
                self.connid = await receive()
                await main_receive_loop()
            except Exception as e:
                logger.exception(e)

            self.connected_evt.clear()

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

    async def ping(self, timeout=10):
        agent = await self.gen_agent()
        await agent.send(PING)
        evt = asyncio.Event()

        async def receive():
            payload = await agent.receive()
            self.agents.pop(agent.msgid)
            if payload == PONG:
                evt.set()

        task1 = asyncio.create_task(receive())

        try:
            await asyncio.wait_for(evt.wait(), timeout)
        except asyncio.TimeoutError:
            pass

        task1.cancel()

        return evt.is_set()

    def remove_agent(self, agent):
        self.agents.pop(agent.msgid, None)

    def close(self, force=False):
        if self._writer:
            self._writer.close()

        if force:
            for task in self._processes:
                task.cancel()

    async def submit_job(self, *args, job=None, **kwargs):
        agent = await self.gen_agent()
        if job is None:
            job = Job(*args, **kwargs)

        job.func = self._add_prefix_subfix(job.func)
        await agent.send(cmd.SubmitJob(job))
        payload = await agent.receive()
        self.remove_agent(agent)
        if payload == cmd.SUCCESS:
            return True
        else:
            return False

    async def run_job(self, *args, job=None, **kwargs):
        agent = await self.gen_agent()
        if job is None:
            job = Job(*args, **kwargs)

        job.func = self._add_prefix_subfix(job.func)
        await agent.send(cmd.RunJob(job))
        payload = await agent.receive()
        self.remove_agent(agent)
        if payload[0] == cmd.NO_WORKER[0]:
            raise Exception('no worker')

        if payload[0] == cmd.DATA[0]:
            return payload[1:]

        return payload

    async def remove_job(self, func, name):
        agent = await self.gen_agent()
        func = self._add_prefix_subfix(func)
        await agent.send(cmd.RemoveJob(func, name))
        payload = await agent.receive()
        self.remove_agent(agent)
        if payload == cmd.SUCCESS:
            return True
        else:
            return False

    async def status(self):
        agent = await self.gen_agent()
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
        agent = await self.gen_agent()
        func = self._add_prefix_subfix(func)
        await agent.send(cmd.DropFunc(func))
        payload = await agent.receive()
        self.remove_agent(agent)
        if payload == cmd.SUCCESS:
            return True
        else:
            return False

    async def connected_wait(self):
        return await self.connected_evt.wait()


class BaseCluster(object):
    def __init__(self, clientclass, entrypoints, *args, **kwargs):
        self.clients = []
        nodes = {}

        for entrypoint in entrypoints:
            client = clientclass(*args, **kwargs)
            self.clients.append(client)
            nodes[entrypoint] = {'hostname': entrypoint, 'instance': client}

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

    def set_prefix(self, prefix):
        self.run_sync('set_prefix', prefix)

    def set_subfix(self, subfix):
        self.run_sync('set_subfix', subfix)

    async def connect(self, connector=None, *args):
        '''connect to servers'''
        for entrypoint, client in zip(self.entrypoints, self.clients):
            await client.connect(connector, entrypoint, *args)

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
