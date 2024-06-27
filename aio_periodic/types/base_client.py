import asyncio
from asyncio import StreamReader, StreamWriter
from .agent import Agent
from .utils import decode_int32, MAGIC_RESPONSE, encode_int32
from .command import PING, PONG, NO_JOB, JOB_ASSIGN, SUCCESS, Command
from . import command as cmd
from binascii import crc32
from .job import Job
from ..transport import Transport
from async_timeout import timeout
from time import time
from typing import Optional, Dict, List, Any, Callable, Coroutine, cast
from mypy_extensions import KwArg, VarArg

try:
    from uhashring import HashRing
except Exception:
    HashRing = None

import logging

logger = logging.getLogger(__name__)


def is_success(payload: bytes) -> bool:
    return payload == SUCCESS


class BaseClient(object):
    connected_evt: asyncio.Event
    connid: bytes | None
    _reader: StreamReader
    _writer: StreamWriter
    msgid_locker: asyncio.Lock
    _send_locker: asyncio.Lock
    _clientType: bytes
    _cb: Callable[[bytes, bytes], Coroutine[Any, Any, None]] | None
    _on_connected: Callable[[], Coroutine[Any, Any, None]] | None
    _on_disconnected: Callable[[], Coroutine[Any, Any, None]] | None
    agents: Dict[bytes, Agent]
    _processes: List[asyncio.Task[Any]]
    prefix: str
    subfix: str
    transport: Transport

    def __init__(
        self,
        clientType: bytes,
        message_callback: Optional[Callable[
            [bytes, bytes],
            Coroutine[Any, Any, Any],
        ]] = None,
        on_connected: Optional[Callable[
            [],
            Coroutine[Any, Any, Any],
        ]] = None,
        on_disconnected: Optional[Callable[
            [],
            Coroutine[Any, Any, Any],
        ]] = None,
    ):

        # self.connected_evt = None

        self.connid = None
        # self._reader = None
        # self._writer = None
        self._buffer = b''
        self._clientType = clientType
        self.agents = dict()
        # self.msgid_locker = None
        self._last_msgid = 0

        self._on_connected = on_connected
        self._on_disconnected = on_disconnected

        self._cb = message_callback
        self._initialized = False
        # self._send_locker = None
        self._processes = []

        self.prefix = ''
        self.subfix = ''

        self.ping_at = time()

    def set_on_connected(self, func: Callable[[], Coroutine[Any, Any,
                                                            Any]]) -> None:
        self._on_connected = func

    def set_on_disconnected(
            self, func: Callable[[], Coroutine[Any, Any, Any]]) -> None:
        self._on_disconnected = func

    def set_prefix(self, prefix: str) -> None:
        self.prefix = prefix

    def set_subfix(self, subfix: str) -> None:
        self.subfix = subfix

    def _add_prefix_subfix(self, func: str) -> str:
        if self.prefix:
            func = f'{self.prefix}{func}'

        if self.subfix:
            func = f'{func}{self.subfix}'

        return func

    def _strip_prefix_subfix(self, func: str) -> str:
        if self.prefix and func.startswith(self.prefix):
            func = func[len(self.prefix):]
        if self.subfix and func.endswith(self.subfix):
            func = func[:-len(self.subfix)]
        return func

    def initialize(self) -> None:
        self._initialized = True
        self.connected_evt = asyncio.Event()
        self._send_locker = asyncio.Lock()
        self.msgid_locker = asyncio.Lock()

    def start_processes(self) -> None:
        task = asyncio.create_task(self.loop_agent())
        self._processes.append(task)
        task = asyncio.create_task(self.check_alive())
        self._processes.append(task)

    def stop_processes(self) -> None:
        for task in self._processes:
            task.cancel()

    async def connect(self, transport: Optional[Transport] = None) -> bool:
        if self._initialized:
            self.close()
        else:
            if not transport:
                raise Exception('connect required transport')
            self.initialize()

        if transport:
            self.transport = transport

        reader, writer = await self.transport.get()
        self._writer = writer
        self._reader = reader
        self._buffer = b''
        agent = Agent(self)
        await agent.send(self._clientType, True)
        self.connected_evt.set()
        self.start_processes()
        if self._on_connected:
            await self._on_connected()

        return True

    def start_connect(self) -> None:

        async def connect_loop() -> None:
            delay = 1
            noconnected = True
            while noconnected:
                try:
                    logger.info('reconnecting...')
                    await self.connect()
                    logger.info('connected')
                    noconnected = False
                except Exception as e:
                    logger.error(f'reconnecting failed: {e}')

                await asyncio.sleep(delay)

        asyncio.create_task(connect_loop())

    @property
    def connected(self) -> bool:
        return self.connected_evt.is_set()

    async def _receive(self, size: int) -> bytes:
        while self.connected:
            if len(self._buffer) >= size:
                buf = self._buffer[:size]
                self._buffer = self._buffer[size:]
                return buf

            is_recv_empty = False
            try:
                async with timeout(100):
                    buf = await self._reader.read(max(4096, size))
                    if len(buf) == 0:
                        is_recv_empty = True

                    self._buffer += buf
            except Exception:
                pass

            if is_recv_empty:
                break

        return b''

    async def check_alive(self) -> None:
        while True:
            await self.connected_evt.wait()
            await asyncio.sleep(5)

            now = time()

            if self.ping_at + 10 > now:
                continue

            if self.ping_at + 120 < now:
                self.connected_evt.clear()
                self._reader.feed_eof()
                continue

            try:
                await self.ping()
            except Exception:
                pass

    def get_next_msgid(self) -> bytes:
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

    def agent(self, timeout: int = 10, autoid: bool = True) -> Agent:
        return Agent(self, timeout, autoid)

    def get_writer(self) -> asyncio.StreamWriter:
        if not self._writer:
            raise Exception('no initialized')
        return self._writer

    async def loop_agent(self) -> None:

        async def receive() -> bytes:
            magic = await self._receive(4)
            if not magic:
                raise Exception("Closed")
            if magic != MAGIC_RESPONSE:
                raise Exception('Magic not match.')
            header = await self._receive(4)
            length = decode_int32(header)
            crc = await self._receive(4)
            payload = await self._receive(length)
            if decode_int32(crc) != crc32(payload):
                raise Exception('CRC not match.')
            return payload

        async def main_receive_loop() -> None:
            while self.connected:
                payload = await receive()
                msgid = payload[0:4]
                payload = payload[4:]

                if payload[0:1] == NO_JOB:
                    continue
                if payload[0:1] == JOB_ASSIGN:
                    if self._cb:
                        await self._cb(payload, msgid)
                    continue

                agent = self.agents.get(msgid)
                if agent:
                    agent.feed_data(payload)
                else:
                    logger.error('Agent %r not found.' % msgid)

        try:
            self.connid = await receive()
            await main_receive_loop()
        finally:
            if self._on_disconnected:
                try:
                    ret = self._on_disconnected
                    if asyncio.iscoroutine(ret):
                        await ret
                except Exception as e:
                    logger.error(f'processing on_disconnected error: {e}')

            self.start_connect()

    async def send_command_and_receive(self,
                                       command: Command | bytes,
                                       parse: Optional[Callable[[bytes],
                                                                Any]] = None,
                                       timeout: int = 10) -> Any | bytes:
        async with self.agent(timeout) as agent:
            await agent.send(command)
            payload = await agent.receive()
            self.ping_at = time()
            if parse:
                return parse(payload)
            else:
                return payload

    async def send_command(self,
                           command: Command | bytes,
                           timeout: int = 10) -> None:
        async with self.agent(timeout, False) as agent:
            await agent.send(command)

    async def ping(self, timeout: int = 10) -> bool:

        def is_pong(payload: bytes) -> bool:
            return payload == PONG

        return cast(
            bool, await self.send_command_and_receive(PING,
                                                      is_pong,
                                                      timeout=timeout))

    def close(self) -> None:
        self._writer.close()

        self.stop_processes()

        for agent in self.agents.values():
            agent.feed_data(b'')

    async def submit_job(self,
                         *args: Any,
                         job: Optional[Job] = None,
                         **kwargs: Any) -> bool:
        if job is None:
            job = Job(*args, **kwargs)

        job.func = self._add_prefix_subfix(job.func)

        return cast(
            bool, await self.send_command_and_receive(cmd.SubmitJob(job),
                                                      is_success))

    async def run_job(self,
                      *args: Any,
                      job: Optional[Job] = None,
                      **kwargs: Any) -> bytes:
        if job is None:
            job = Job(*args, **kwargs)

        job.func = self._add_prefix_subfix(job.func)

        if job.timeout == 0:
            job.timeout = 10

        timeout = job.timeout

        def parse(payload: bytes) -> bytes:
            if payload[0] == cmd.NO_WORKER[0]:
                raise Exception('no worker')

            if payload[0] == cmd.DATA[0]:
                return payload[1:]

            return payload

        return cast(
            bytes, await self.send_command_and_receive(cmd.RunJob(job), parse,
                                                       timeout))

    async def remove_job(self, func: str, name: Any) -> bool:
        func = self._add_prefix_subfix(func)
        command = cmd.RemoveJob(func, name)
        return cast(bool, await
                    self.send_command_and_receive(command, is_success))

    async def status(self) -> Any:

        def parse(payload: bytes) -> Any:
            payload_s = str(payload, 'utf-8').strip()
            stats = payload_s.split('\n')
            retval = {}
            for stat_s in stats:
                stat_s = stat_s.strip()
                if not stat_s:
                    continue
                stat = stat_s.split(',')
                retval[stat[0]] = {
                    'func_name': stat[0],
                    'worker_count': int(stat[1]),
                    'job_count': int(stat[2]),
                    'processing': int(stat[3]),
                    'locked': int(stat[4]),
                    'sched_at': int(stat[5])
                }

            return retval

        return await self.send_command_and_receive(cmd.Status(), parse)

    async def drop_func(self, func: str) -> bool:
        func = self._add_prefix_subfix(func)
        return cast(
            bool, await self.send_command_and_receive(cmd.DropFunc(func),
                                                      is_success))

    async def connected_wait(self) -> None:
        await self.connected_evt.wait()


class BaseCluster(object):
    clients: List[BaseClient]
    entrypoints: List[str]
    hr: HashRing

    def __init__(
        self,
        clientclass: Callable[[VarArg(Any), KwArg(Any)], BaseClient],
        entrypoints: List[str],
        *args: Any,
        **kwargs: Any,
    ) -> None:
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

    def get(self, name: str) -> BaseClient:
        '''get one client by hashring'''
        return cast(BaseClient, self.hr[name])

    async def run(self,
                  method_name: str,
                  *args: Any,
                  reduce: Optional[Callable[[Any, Any], Any]] = None,
                  initialize: Optional[Any] = None,
                  **kwargs: Any) -> Any:
        retval = initialize
        for client in self.clients:
            method = getattr(client, method_name)
            ret = await method(*args, **kwargs)
            if reduce:
                retval = reduce(retval, ret)

        return retval

    def run_sync(self,
                 method_name: str,
                 *args: Any,
                 reduce: Optional[Callable[[Any, Any], Any]] = None,
                 initialize: Optional[Any] = None,
                 **kwargs: Any) -> Any:
        retval = initialize
        for client in self.clients:
            method = getattr(client, method_name)
            ret = method(*args, **kwargs)
            if reduce:
                retval = reduce(retval, ret)

        return retval

    def set_on_connected(self, func: str) -> None:
        self.run_sync('set_on_connected', func)

    def set_on_disconnected(self, func: str) -> None:
        self.run_sync('set_on_disconnected', func)

    def set_prefix(self, prefix: str) -> None:
        self.run_sync('set_prefix', prefix)

    def set_subfix(self, subfix: str) -> None:
        self.run_sync('set_subfix', subfix)

    async def connect(self, transports: Dict[str, Transport]) -> None:
        '''connect to servers'''
        for entrypoint, client in zip(self.entrypoints, self.clients):
            transport = transports.get(entrypoint)
            if not transport:
                raise Exception('no transport ' + entrypoint)

            await client.connect(transport)

    def close(self) -> None:
        '''close all the servers'''
        self.run_sync('close')

    async def submit_job(self,
                         *args: Any,
                         job: Optional[Job] = None,
                         **kwargs: Any) -> bool:
        '''submit job to one server'''
        if job is None:
            job = Job(*args, **kwargs)
        client = self.get(job.name)
        return await client.submit_job(job=job)

    async def run_job(self,
                      *args: Any,
                      job: Optional[Job] = None,
                      **kwargs: Any) -> bytes:
        '''run job to one server'''
        if job is None:
            job = Job(*args, **kwargs)
        client = self.get(job.name)
        return await client.run_job(job=job)

    async def remove_job(self, func: str, name: str) -> bool:
        '''remove job from servers'''
        client = self.get(name)
        return await client.remove_job(func, name)

    async def drop_func(self, func: str) -> None:
        '''drop func from servers'''
        await self.run('drop_func', func)

    async def status(self) -> Any:
        '''status from servers and merge the result'''

        def reduce(stats: Any, stat: Any) -> Any:
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
