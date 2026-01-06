import asyncio
import logging
from binascii import crc32
from time import time
from typing import (Optional, Dict, List, Any, Callable, Coroutine, Union,
                    cast, AsyncIterable)
from asyncio import StreamReader, StreamWriter

# Internal imports
from .agent import Agent
from .utils import decode_int32, MAGIC_RESPONSE, encode_int32
from .command import PING, PONG, NO_JOB, JOB_ASSIGN, SUCCESS, Command
from . import command as cmd
from .job import Job
from ..transport import BaseTransport

try:
    from uhashring import HashRing  # type: ignore
except ImportError:
    HashRing = None

logger = logging.getLogger(__name__)


def is_success(payload: bytes) -> bool:
    return payload == SUCCESS


OnConnectedFunc = Callable[[], Coroutine[Any, Any, None]]
OnDisconnectedFunc = Callable[[], Coroutine[Any, Any, None]]
MessageCallbackFunc = Callable[[bytes, bytes], Coroutine[Any, Any, None]]
ParseFunc = Callable[[bytes], Any]

SyncRunJobStreamFunc = Callable[[bytes], None]
AsyncRunJobStreamFunc = Callable[[bytes], Coroutine[Any, Any, Any]]
RunJobStreamFunc = Union[SyncRunJobStreamFunc, AsyncRunJobStreamFunc]


class BaseClient(object):
    connected_evt: asyncio.Event
    connid: Optional[bytes]
    _reader: StreamReader
    _writer: StreamWriter
    msgid_locker: asyncio.Lock
    send_locker: asyncio.Lock
    _clientType: bytes
    _cb: Optional[MessageCallbackFunc]
    _on_connected: Optional[OnConnectedFunc]
    _on_disconnected: Optional[OnDisconnectedFunc]
    agents: Dict[bytes, Agent]
    _processes: List[asyncio.Task[Any]]
    prefix: str
    subfix: str
    transport: BaseTransport

    def __init__(
        self,
        clientType: bytes,
        message_callback: Optional[MessageCallbackFunc] = None,
        on_connected: Optional[OnConnectedFunc] = None,
        on_disconnected: Optional[OnDisconnectedFunc] = None,
    ):
        self.connid = None
        self._clientType = clientType
        self.agents = {}
        self._last_msgid = 0

        self._on_connected = on_connected
        self._on_disconnected = on_disconnected
        self._cb = message_callback

        self._initialized = False
        self._processes = []

        self.prefix = ''
        self.subfix = ''
        self.ping_at = time()

    def set_on_connected(self, func: OnConnectedFunc) -> None:
        self._on_connected = func

    def set_on_disconnected(self, func: OnDisconnectedFunc) -> None:
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
        self.send_locker = asyncio.Lock()
        self.msgid_locker = asyncio.Lock()

    def start_processes(self) -> None:
        # Create background tasks for message loop and health check
        self._processes.append(asyncio.create_task(self.loop_agent()))
        self._processes.append(asyncio.create_task(self.check_alive()))

    def stop_processes(self) -> None:
        for task in self._processes:
            task.cancel()
        self._processes.clear()

    async def connect(self, transport: Optional[BaseTransport] = None) -> bool:
        if self._initialized:
            self.close()
        else:
            if not transport:
                raise Exception('Transport required for initial connection')
            self.initialize()

        if transport:
            self.transport = transport

        # Establish connection via transport layer
        reader, writer = await self.transport.get()
        self._writer = writer
        self._reader = reader

        # Handshake: Send client type
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
            not_connected = True
            while not_connected:
                try:
                    logger.info('Reconnecting...')
                    await self.connect()
                    logger.info('Connected')
                    not_connected = False
                except Exception as e:
                    logger.error(f'Reconnect failed: {e}')

                if not_connected:
                    await asyncio.sleep(delay)

        asyncio.create_task(connect_loop())

    @property
    def connected(self) -> bool:
        return self.connected_evt.is_set()

    async def check_alive(self) -> None:
        """Periodically ping the server to ensure connection health."""
        while True:
            await self.connected_evt.wait()
            await asyncio.sleep(5)

            now = time()

            # Skip if recent activity
            if self.ping_at + 10 > now:
                continue

            # Connection timeout
            if self.ping_at + 120 < now:
                logger.warning("Connection timed out. Resetting.")
                self.connected_evt.clear()
                # Determine how to close based on available method
                if hasattr(self._reader, 'feed_eof'):
                    self._reader.feed_eof()
                # Alternatively, close writer to trigger reconnection
                self.close()
                continue

            try:
                await self.ping()
            except Exception:
                # Ping failed, loop will retry or timeout eventually
                pass

    def get_next_msgid(self) -> bytes:
        """Generate a unique message ID for agents."""
        # Limit loops to prevent infinite hang in bad state
        for _ in range(1_000_000):
            self._last_msgid += 1
            if self._last_msgid > 0xFFFFFF00:
                self._last_msgid = 0

            # Log stats periodically
            if self._last_msgid % 100_000 == 0:
                count = len(self.agents)
                logger.info(f'Last msgid {self._last_msgid} Agents {count}')

            msgid = encode_int32(self._last_msgid)
            if msgid not in self.agents:
                return msgid

        raise Exception('No available msgid found')

    def agent(self, timeout: int = 10, autoid: bool = True) -> Agent:
        return Agent(self, timeout, autoid)

    def get_writer(self) -> asyncio.StreamWriter:
        if not self._writer:
            raise Exception('Client not initialized')
        return self._writer

    async def loop_agent(self) -> None:
        """Main loop handling incoming messages from the server."""

        async def receive_exact(n: int) -> bytes:
            try:
                # Use readexactly for efficient buffering and EOF handling
                data = await self._reader.readexactly(n)
                return data
            except asyncio.IncompleteReadError:
                raise Exception("Connection closed by peer")

        async def receive_packet() -> bytes:
            magic = await receive_exact(4)
            if magic != MAGIC_RESPONSE:
                raise Exception('Magic mismatch')

            header = await receive_exact(4)
            length = decode_int32(header)

            crc = await receive_exact(4)
            payload = await receive_exact(length)

            if decode_int32(crc) != crc32(payload):
                raise Exception('CRC mismatch')
            return payload

        try:
            # First packet is always the connection ID
            self.connid = await receive_packet()

            while self.connected:
                payload = await receive_packet()

                # Parse Message ID (first 4 bytes)
                msgid = payload[0:4]
                payload = payload[4:]

                # Handle specific commands
                command_byte = payload[0:1]

                if command_byte == NO_JOB:
                    continue

                if command_byte == JOB_ASSIGN:
                    if self._cb:
                        await self._cb(payload, msgid)
                    continue

                # Route data to the specific agent waiting for it
                agent = self.agents.get(msgid)
                if agent:
                    agent.feed_data(payload)
                else:
                    logger.error(f'Agent {msgid!r} not found.')

        except (asyncio.CancelledError, Exception) as e:
            # Handle disconnection
            if not isinstance(e, asyncio.CancelledError):
                logger.error(f"Loop agent error: {e}")
        finally:
            if self._on_disconnected:
                try:
                    ret = self._on_disconnected()
                    if asyncio.iscoroutine(ret):
                        await ret
                except Exception as e:
                    logger.error(f'on_disconnected error: {e}')

            # Trigger reconnection logic
            self.start_connect()

    async def send_command_and_receive(self,
                                       command: Command | bytes,
                                       parse: Optional[ParseFunc] = None,
                                       timeout: int = 10) -> Any | bytes:
        async with self.agent(timeout) as agent:
            await agent.send(command)
            payload = await agent.receive()
            self.ping_at = time()  # Update activity timestamp
            if parse:
                return parse(payload)
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
        if self._writer:
            self._writer.close()

        self.stop_processes()

        # Release any waiting agents
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
                      stream: Optional[RunJobStreamFunc] = None,
                      **kwargs: Any) -> bytes:
        if job is None:
            job = Job(*args, **kwargs)

        if job.timeout == 0:
            job.timeout = 10

        timeout = job.timeout

        def parse(payload: bytes) -> bytes:
            if payload.startswith(cmd.NO_WORKER):
                raise Exception('no worker')
            if payload.startswith(cmd.DATA):
                return payload[1:]
            return payload

        task: Optional[asyncio.Task[None]] = None
        if stream is not None:
            # Handle streaming response
            func = job.func
            name = job.name
            fut: asyncio.Future[bool] = asyncio.Future()

            async def do_stream_task() -> None:
                async for data in self.recv_job_data(func, name, timeout, fut):
                    ret = stream(data)
                    if asyncio.iscoroutine(ret):
                        await ret

            task = asyncio.create_task(do_stream_task())
            # Wait for stream setup if needed, but here we proceed to send cmd
            # Ideally we rely on the future 'fut' being set by recv loop

        job.func = self._add_prefix_subfix(job.func)
        rj = cmd.RunJob(job)

        try:
            ret = await self.send_command_and_receive(rj, parse, timeout)
            if task:
                # Wait for stream to finish if successful
                await fut
        finally:
            if task and not task.done():
                task.cancel()

        if task:
            await task

        return cast(bytes, ret)

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
                parts = stat_s.split(',')
                retval[parts[0]] = {
                    'func_name': parts[0],
                    'worker_count': int(parts[1]),
                    'job_count': int(parts[2]),
                    'processing': int(parts[3]),
                    'locked': int(parts[4]),
                    'sched_at': int(parts[5])
                }
            return retval

        return await self.send_command_and_receive(cmd.Status(), parse)

    async def drop_func(self, func: str) -> bool:
        func = self._add_prefix_subfix(func)
        return cast(
            bool, await self.send_command_and_receive(cmd.DropFunc(func),
                                                      is_success))

    async def recv_job_data(
        self,
        func: str,
        name: str,
        timeout: int = 120,
        fut: Optional[asyncio.Future[bool]] = None,
    ) -> AsyncIterable[bytes]:
        job = Job(func, name)
        job.func = self._add_prefix_subfix(job.func)

        async with self.agent(timeout) as agent:
            await agent.send(cmd.RecvData(job))
            while True:
                payload = await agent.receive()

                if payload.startswith(cmd.NO_WORKER):
                    raise Exception('no worker')

                if payload.startswith(cmd.DATA):
                    payload = payload[1:]
                    if payload == b'EOF':
                        break
                    yield payload

                if payload.startswith(cmd.SUCCESS):
                    if fut:
                        fut.set_result(True)

    async def connected_wait(self) -> None:
        await self.connected_evt.wait()


class BaseCluster(object):
    clients: List[BaseClient]
    entrypoints: List[str]
    hr: Any  # HashRing type

    def __init__(
        self,
        clientclass: Callable[..., BaseClient],
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
        """Get one client by hashring."""
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

    def set_on_connected(self, func: Any) -> None:
        self.run_sync('set_on_connected', func)

    def set_on_disconnected(self, func: Any) -> None:
        self.run_sync('set_on_disconnected', func)

    def set_prefix(self, prefix: str) -> None:
        self.run_sync('set_prefix', prefix)

    def set_subfix(self, subfix: str) -> None:
        self.run_sync('set_subfix', subfix)

    async def connect(self, transports: Dict[str, BaseTransport]) -> None:
        """Connect to all servers."""
        for entrypoint, client in zip(self.entrypoints, self.clients):
            transport = transports.get(entrypoint)
            if not transport:
                raise Exception(f'No transport for {entrypoint}')
            await client.connect(transport)

    def close(self) -> None:
        """Close all server connections."""
        self.run_sync('close')

    async def submit_job(self,
                         *args: Any,
                         job: Optional[Job] = None,
                         **kwargs: Any) -> bool:
        """Submit job to one server based on hashing."""
        if job is None:
            job = Job(*args, **kwargs)
        client = self.get(job.name)
        return await client.submit_job(job=job)

    async def run_job(self,
                      *args: Any,
                      job: Optional[Job] = None,
                      stream: Optional[RunJobStreamFunc] = None,
                      **kwargs: Any) -> bytes:
        """Run job on one server based on hashing."""
        if job is None:
            job = Job(*args, **kwargs)
        client = self.get(job.name)
        return await client.run_job(job=job, stream=stream)

    async def recv_job_data(
        self,
        func: str,
        name: str,
        timeout: int = 120,
        fut: Optional[asyncio.Future[bool]] = None,
    ) -> AsyncIterable[bytes]:
        """Receive job data from one server."""
        client = self.get(name)
        async for v in client.recv_job_data(func, name, timeout, fut):
            yield v

    async def remove_job(self, func: str, name: str) -> bool:
        """Remove job from servers."""
        client = self.get(name)
        return await client.remove_job(func, name)

    async def drop_func(self, func: str) -> None:
        """Drop func from all servers."""
        await self.run('drop_func', func)

    async def status(self) -> Any:
        """Get status from all servers and merge results."""

        def reduce(stats: Any, stat: Any) -> Any:
            for func in stat.keys():
                if not stats.get(func):
                    stats[func] = stat[func]
                else:
                    s_ptr = stats[func]
                    n_ptr = stat[func]
                    s_ptr['worker_count'] += n_ptr['worker_count']
                    s_ptr['job_count'] += n_ptr['job_count']
                    s_ptr['processing'] += n_ptr['processing']
                    if s_ptr['sched_at'] > n_ptr['sched_at']:
                        s_ptr['sched_at'] = n_ptr['sched_at']
            return stats

        return await self.run('status', reduce=reduce, initialize={})
