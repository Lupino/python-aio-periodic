"""Provide asynchronous periodic-server clients and hash-ring routing."""

import asyncio
import logging
from binascii import crc32
from contextlib import suppress
from time import time
from typing import (Awaitable, Optional, Dict, List, Callable, Union, cast,
                    AsyncIterable, TypeVar, Protocol, TypedDict, overload)
from asyncio import StreamReader, StreamWriter

# Internal imports
from .agent import Agent
from .utils import (decode_int32, MAGIC_RESPONSE, encode_int32, to_str,
                    build_client_registration)
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
    """Return whether a server payload represents a successful command.

    Args:
        payload: Raw command response payload.

    Returns:
        Whether the payload is the protocol success marker.
    """
    return payload == SUCCESS


class FuncStatus(TypedDict):
    """Describe the server-reported state for a registered function."""

    func_name: str
    worker_count: int
    job_count: int
    processing: int
    locked: int
    sched_at: int


StatusMap = Dict[str, FuncStatus]

# Connection lifecycle callbacks are awaited so setup and teardown finish before
# the client advances to the next connection state.
OnConnectedFunc = Callable[[], Awaitable[None]]
OnDisconnectedFunc = Callable[[], Awaitable[None]]
MessageCallbackFunc = Callable[[bytes, bytes], Awaitable[None]]

# Streaming callers may process chunks inline or return a coroutine when their
# handling requires asynchronous I/O.
SyncRunJobStreamFunc = Callable[[bytes], None]
AsyncRunJobStreamFunc = Callable[[bytes], Awaitable[None]]
RunJobStreamFunc = Union[SyncRunJobStreamFunc, AsyncRunJobStreamFunc]
ParseRet = TypeVar('ParseRet')
RunRet = TypeVar('RunRet')
ReduceRet = TypeVar('ReduceRet')


class HashRingLike(Protocol):
    """Define the hash-ring interface used to select a client."""

    def __getitem__(self, key: str) -> 'BaseClient':
        ...


class BaseClient(object):
    """Manage one asynchronous connection to a periodic server.

    The client owns protocol framing, request-agent dispatch, connection health
    checks, and reconnect coordination for one transport endpoint.
    """
    connected_evt: asyncio.Event
    connid: Optional[bytes]
    _reader: Optional[StreamReader]
    _writer: Optional[StreamWriter]
    msgid_locker: asyncio.Lock
    send_locker: asyncio.Lock
    _clientType: bytes
    _cb: Optional[MessageCallbackFunc]
    _on_connected: Optional[OnConnectedFunc]
    _on_disconnected: Optional[OnDisconnectedFunc]
    agents: Dict[bytes, Agent]
    _processes: List[asyncio.Task[None]]
    prefix: str
    subfix: str
    transport: BaseTransport
    _closing: bool
    _reconnect_requested: bool
    _reconnect_task: Optional[asyncio.Task[None]]

    def __init__(
        self,
        clientType: bytes,
        message_callback: Optional[MessageCallbackFunc] = None,
        on_connected: Optional[OnConnectedFunc] = None,
        on_disconnected: Optional[OnDisconnectedFunc] = None,
        client_name: Optional[str] = None,
        client_token: Optional[str] = None,
    ):
        """Initialize client state before a transport is connected.

        Args:
            clientType: Protocol registration bytes for this client.
            message_callback: Handler for server-initiated job assignments.
            on_connected: Coroutine invoked after a successful handshake.
            on_disconnected: Coroutine invoked when the receive loop ends.
            client_name: Optional client name added to registration bytes.
            client_token: Optional client token added to registration bytes.
        """
        self.connid = None
        self._clientType = build_client_registration(clientType, client_name,
                                                     client_token)
        self.agents = {}
        self._last_msgid = 0

        self._on_connected = on_connected
        self._on_disconnected = on_disconnected
        self._cb = message_callback

        self._initialized = False
        # Keep these ready so properties like `connected` are safe before
        # the first `connect()` call.
        self.connected_evt = asyncio.Event()
        self.send_locker = asyncio.Lock()
        self.msgid_locker = asyncio.Lock()
        self._reader = None
        self._writer = None
        self._processes = []
        self._closing = False
        self._reconnect_requested = False
        self._reconnect_task = None

        self.prefix = ''
        self.subfix = ''
        self.ping_at = time()

    def set_on_connected(self, func: OnConnectedFunc) -> None:
        """Set the coroutine invoked after a connection is established.

        Args:
            func: Callback to invoke after the protocol handshake completes.
        """
        self._on_connected = func

    def set_on_disconnected(self, func: OnDisconnectedFunc) -> None:
        """Set the coroutine invoked when the connection receive loop ends.

        Args:
            func: Callback to invoke after a connection is lost or closed.
        """
        self._on_disconnected = func

    def set_prefix(self, prefix: str) -> None:
        """Set the string prepended to submitted function names.

        Args:
            prefix: Namespace prefix applied before sending function names.
        """
        self.prefix = prefix

    def set_subfix(self, subfix: str) -> None:
        """Set the string appended to submitted function names.

        Args:
            subfix: Namespace suffix applied before sending function names.
        """
        self.subfix = subfix

    def _add_prefix_subfix(self, func: str) -> str:
        """Apply the configured function-name namespace.

        Args:
            func: Function name without the configured namespace.

        Returns:
            Function name with the configured prefix and suffix.
        """
        if self.prefix:
            func = f'{self.prefix}{func}'
        if self.subfix:
            func = f'{func}{self.subfix}'
        return func

    def _strip_prefix_subfix(self, func: str) -> str:
        """Remove the configured function-name namespace when present.

        Args:
            func: Potentially namespaced function name.

        Returns:
            Function name without matching configured prefix and suffix.
        """
        if self.prefix and func.startswith(self.prefix):
            func = func[len(self.prefix):]
        if self.subfix and func.endswith(self.subfix):
            func = func[:-len(self.subfix)]
        return func

    def initialize(self) -> None:
        """Reset state required for a new connection lifecycle."""
        self._initialized = True
        self.connected_evt = asyncio.Event()
        self.send_locker = asyncio.Lock()
        self.msgid_locker = asyncio.Lock()
        self._closing = False
        self._reconnect_requested = False

    def start_processes(self) -> None:
        """Start the receive loop and the connection health-check task."""
        # Each connection lifecycle gets its own receive and health-check tasks.
        self._processes.append(asyncio.create_task(self.loop_agent()))
        self._processes.append(asyncio.create_task(self.check_alive()))

    def stop_processes(self) -> None:
        """Cancel and forget background tasks for the current lifecycle."""
        for task in self._processes:
            task.cancel()
        self._processes.clear()

    async def connect(self, transport: Optional[BaseTransport] = None) -> bool:
        """Connect to the server and complete the client registration handshake.

        Args:
            transport: Transport for the first connection; later reconnects reuse
                the existing transport.

        Returns:
            Whether the handshake completed successfully.

        Raises:
            Exception: If an initial connection has no transport or the transport
                cannot establish a valid connection.
        """
        if self._initialized:
            # Reset current state before reconnecting, but keep an active
            # reconnect loop alive if this connect() is invoked from it.
            self.close(_cancel_reconnect_task=False)
            # Ensure reconnect loop keeps retrying if this connect attempt
            # fails before transport establishment.
            self._closing = False
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
        self._closing = False
        self._reconnect_requested = False

        # Handshake: Send client type
        agent = Agent(self)
        await agent.send(self._clientType, True)

        self.connected_evt.set()
        self.start_processes()

        if self._on_connected:
            await self._on_connected()

        return True

    def start_connect(self) -> None:
        """Start one reconnect loop unless one is already active."""
        if self._reconnect_task and not self._reconnect_task.done():
            return

        async def connect_loop() -> None:
            try:
                delay = 1
                not_connected = True
                while not_connected and not self._closing:
                    try:
                        logger.info('Reconnecting...')
                        await self.connect()
                        logger.info('Connected')
                        not_connected = False
                    except Exception as e:
                        logger.error(f'Reconnect failed: {e}')

                    if not_connected:
                        await asyncio.sleep(delay)
            finally:
                self._reconnect_task = None

        self._reconnect_task = asyncio.create_task(connect_loop())

    @property
    def connected(self) -> bool:
        """Return whether the registration handshake has completed."""
        return self.connected_evt.is_set()

    async def check_alive(self) -> None:
        """Periodically ping the server to ensure connection health."""
        while True:
            await self.connected_evt.wait()
            await asyncio.sleep(5)

            now = time()

            # Any command response refreshes ping_at, so an extra ping is needless.
            if self.ping_at + 10 > now:
                continue

            # A stale connection must be torn down so the receive loop can reconnect.
            if self.ping_at + 120 < now:
                logger.warning("Connection timed out. Resetting.")
                self.connected_evt.clear()
                # Determine how to close based on available method
                reader = self._reader
                if reader is not None and hasattr(reader, 'feed_eof'):
                    reader.feed_eof()
                # Alternatively, close writer to trigger reconnection
                self.close(reconnect=True)
                continue

            try:
                await self.ping()
            except Exception:
                # Ping failed, loop will retry or timeout eventually
                pass

    def get_next_msgid(self) -> bytes:
        """Generate a unique message ID for agents."""
        # Bound collision probing in case outstanding agents exhaust the ID space.
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
        """Create an agent that owns one command exchange.

        Args:
            timeout: Maximum seconds to wait for an agent response.
            autoid: Whether the agent should allocate a protocol message ID.

        Returns:
            A command agent bound to this client.
        """
        return Agent(self, timeout, autoid)

    def get_writer(self) -> asyncio.StreamWriter:
        """Return the active transport writer.

        Returns:
            Stream writer for the active transport.

        Raises:
            Exception: If the client has not established a transport.
        """
        if not self._writer:
            raise Exception('Client not initialized')
        return self._writer

    async def loop_agent(self) -> None:
        """Main loop handling incoming messages from the server."""
        reader = self._reader
        if reader is None:
            raise Exception('Client reader is not initialized')

        async def receive_exact(n: int) -> bytes:
            try:
                # Exact reads preserve protocol frame boundaries and surface EOF.
                data = await reader.readexactly(n)
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
            # The server sends a connection ID before any multiplexed response.
            self.connid = await receive_packet()

            while self.connected:
                payload = await receive_packet()

                # The first four bytes identify the agent awaiting this response.
                msgid = payload[0:4]
                payload = payload[4:]

                # Job assignments are server-initiated; other payloads target agents.
                command_byte = payload[0:1]

                if command_byte == NO_JOB:
                    continue

                if command_byte == JOB_ASSIGN:
                    if self._cb:
                        await self._cb(payload, msgid)
                    continue

                agent = self.agents.get(msgid)
                if agent:
                    agent.feed_data(payload)
                else:
                    logger.error(f'Agent {msgid!r} not found.')

        except (asyncio.CancelledError, Exception) as e:
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

            # Explicit close suppresses reconnect unless the caller requested it.
            reconnect = (not self._closing) or self._reconnect_requested
            self._closing = False
            self._reconnect_requested = False
            if reconnect:
                self.start_connect()

    @overload
    async def send_command_and_receive(
        self,
        command: Command | bytes,
        parse: None = None,
        timeout: int = 10,
    ) -> bytes:
        ...

    @overload
    async def send_command_and_receive(
        self,
        command: Command | bytes,
        parse: Callable[[bytes], ParseRet],
        timeout: int = 10,
    ) -> ParseRet:
        ...

    async def send_command_and_receive(
        self,
        command: Command | bytes,
        parse: Optional[Callable[[bytes], ParseRet]] = None,
        timeout: int = 10,
    ) -> ParseRet | bytes:
        """Send a command and return its response, optionally parsed.

        Args:
            command: Protocol command or encoded command bytes to send.
            parse: Optional converter for the raw response payload.
            timeout: Maximum seconds to await the response.

        Returns:
            Raw response payload or the value returned by ``parse``.
        """
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
        """Send a command that does not require a response.

        Args:
            command: Protocol command or encoded command bytes to send.
            timeout: Maximum seconds to retain the sending agent.
        """
        async with self.agent(timeout, False) as agent:
            await agent.send(command)

    async def ping(self, timeout: int = 10) -> bool:
        """Send a protocol ping and verify the matching pong response.

        Args:
            timeout: Maximum seconds to await the pong.

        Returns:
            Whether the server returned a valid pong response.
        """

        def is_pong(payload: bytes) -> bool:
            return payload == PONG

        return await self.send_command_and_receive(PING,
                                                   is_pong,
                                                   timeout=timeout)

    def close(
        self,
        reconnect: bool = False,
        _cancel_reconnect_task: bool = True,
    ) -> None:
        """Close the connection and release all pending command agents.

        Args:
            reconnect: Whether to start a new connection after closing.
            _cancel_reconnect_task: Internal flag that preserves the active retry
                loop while it attempts another connection.
        """
        self._closing = True
        self._reconnect_requested = reconnect
        self.connected_evt.clear()

        if self._writer:
            self._writer.close()

        if (_cancel_reconnect_task and self._reconnect_task
                and not self._reconnect_task.done()):
            self._reconnect_task.cancel()
            self._reconnect_task = None

        self.stop_processes()

        # Empty payloads wake agents so their callers do not wait indefinitely.
        for agent in list(self.agents.values()):
            agent.feed_data(b'')

    async def submit_job(
        self,
        func: str | bytes = '',
        name: str | bytes = '',
        workload: bytes = b'',
        sched_at: int = 0,
        count: int = 0,
        timeout: int = 0,
        job: Optional[Job] = None,
    ) -> bool:
        """Submit a job to the connected server.

        Args:
            func: Function name for a newly created job.
            name: Job name for a newly created job.
            workload: Serialized job payload.
            sched_at: Unix timestamp at which to schedule the job.
            count: Number of times to run the job.
            timeout: Job timeout in seconds.
            job: Existing job object, which takes precedence over individual fields.

        Returns:
            Whether the server accepted the job.
        """
        if job is None:
            job = Job(func, name, workload, sched_at, count, timeout)

        job.func = self._add_prefix_subfix(to_str(job.func))
        return await self.send_command_and_receive(cmd.SubmitJob(job),
                                                   is_success)

    async def run_job(
        self,
        func: str | bytes = '',
        name: str | bytes = '',
        workload: bytes = b'',
        sched_at: int = 0,
        count: int = 0,
        timeout: int = 0,
        job: Optional[Job] = None,
        stream: Optional[RunJobStreamFunc] = None,
    ) -> bytes:
        """Run a job and optionally forward its streamed data.

        Args:
            func: Function name for a newly created job.
            name: Job name for a newly created job.
            workload: Serialized job payload.
            sched_at: Unix timestamp at which to schedule the job.
            count: Number of times to run the job.
            timeout: Job timeout in seconds.
            job: Existing job object, which takes precedence over individual fields.
            stream: Optional synchronous or asynchronous callback for job data.

        Returns:
            Final job response payload.
        """
        if job is None:
            job = Job(func, name, workload, sched_at, count, timeout)

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
            # Start receiving before dispatch so early data is not missed.
            func = to_str(job.func)
            name = to_str(job.name)
            fut: asyncio.Future[bool] = asyncio.Future()

            async def do_stream_task() -> None:
                async for data in self.recv_job_data(func, name, timeout, fut):
                    ret = stream(data)
                    if asyncio.iscoroutine(ret):
                        await ret

            task = asyncio.create_task(do_stream_task())

        job.func = self._add_prefix_subfix(to_str(job.func))
        rj = cmd.RunJob(job)

        try:
            ret = await self.send_command_and_receive(rj, parse, timeout)
            if task:
                # The final response may arrive before the stream EOF marker.
                await fut
        finally:
            if task and not task.done():
                task.cancel()

        if task:
            with suppress(asyncio.CancelledError):
                await task

        return ret

    async def remove_job(self, func: str, name: str | bytes) -> bool:
        """Remove one named job from the connected server.

        Args:
            func: Function that owns the job.
            name: Name of the job to remove.

        Returns:
            Whether the server removed the job.
        """
        func = self._add_prefix_subfix(func)
        command = cmd.RemoveJob(func, name)
        return await self.send_command_and_receive(command, is_success)

    async def status(self) -> StatusMap:
        """Return the server status indexed by function name.

        Returns:
            Parsed worker and job counts for each function.
        """

        def parse(payload: bytes) -> StatusMap:
            payload_s = str(payload, 'utf-8').strip()
            stats = payload_s.split('\n')
            retval: StatusMap = {}
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
        """Remove all jobs and workers for a function.

        Args:
            func: Function name to drop.

        Returns:
            Whether the server accepted the operation.
        """
        func = self._add_prefix_subfix(func)
        return await self.send_command_and_receive(cmd.DropFunc(func),
                                                   is_success)

    async def recv_job_data(
        self,
        func: str,
        name: str,
        timeout: int = 120,
        fut: Optional[asyncio.Future[bool]] = None,
    ) -> AsyncIterable[bytes]:
        """Yield streamed data for a job until the server signals completion.

        Args:
            func: Function that owns the job.
            name: Name of the job producing streamed data.
            timeout: Maximum seconds to await each stream response.
            fut: Optional future completed after the server success marker.

        Yields:
            Data chunks emitted by the job.
        """
        job = Job(func, name)
        job.func = self._add_prefix_subfix(to_str(job.func))

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
        """Wait until the client completes its connection handshake."""
        await self.connected_evt.wait()


class BaseCluster(object):
    """Route periodic-server operations across clients using consistent hashing."""
    clients: List[BaseClient]
    entrypoints: List[str]
    hr: HashRingLike

    def __init__(
        self,
        clientclass: Callable[..., BaseClient],
        entrypoints: List[str],
        *args: object,
        **kwargs: object,
    ) -> None:
        """Create one client per entrypoint and initialize the hash ring.

        Args:
            clientclass: Client implementation to instantiate per entrypoint.
            entrypoints: Server addresses used as consistent-hash nodes.
            *args: Positional arguments forwarded to each client constructor.
            **kwargs: Keyword arguments forwarded to each client constructor.

        Raises:
            Exception: If the optional hash-ring dependency is unavailable.
        """
        self.clients = []
        nodes: Dict[str, Dict[str, object]] = {}

        for entrypoint in entrypoints:
            client = clientclass(*args, **kwargs)
            self.clients.append(client)
            nodes[entrypoint] = {'hostname': entrypoint, 'instance': client}

        if HashRing is None:
            raise Exception('Please install uhashring library.')

        self.entrypoints = entrypoints
        self.hr = HashRing(nodes=nodes, hash_fn='ketama')

    def get(self, name: str) -> BaseClient:
        """Select the client responsible for a hash key.

        Args:
            name: Key used for consistent-hash selection.

        Returns:
            Client assigned to the key.
        """
        return self.hr[name]

    async def run(self,
                  method_name: str,
                  *args: object,
                  reduce: Optional[Callable[[ReduceRet, RunRet],
                                            ReduceRet]] = None,
                  initialize: Optional[ReduceRet] = None,
                  **kwargs: object) -> ReduceRet | RunRet | None:
        """Call an asynchronous client method on every cluster member.

        Args:
            method_name: Name of the asynchronous client method to invoke.
            *args: Positional arguments forwarded to each method call.
            reduce: Optional accumulator for every method result.
            initialize: Initial value required when ``reduce`` is provided.
            **kwargs: Keyword arguments forwarded to each method call.

        Returns:
            The last result, reduced result, or ``None`` for an empty cluster.

        Raises:
            ValueError: If a reducer is provided without an initial value.
        """
        if reduce is None:
            last: Optional[RunRet] = None
            for client in self.clients:
                method = cast(Callable[..., Awaitable[RunRet]],
                              getattr(client, method_name))
                last = await method(*args, **kwargs)
            return last

        if initialize is None:
            raise ValueError('initialize is required when reduce is provided')

        retval: ReduceRet = initialize
        for client in self.clients:
            method = cast(Callable[..., Awaitable[RunRet]],
                          getattr(client, method_name))
            ret = await method(*args, **kwargs)
            retval = reduce(retval, ret)
        return retval

    def run_sync(self,
                 method_name: str,
                 *args: object,
                 reduce: Optional[Callable[[ReduceRet, RunRet],
                                           ReduceRet]] = None,
                 initialize: Optional[ReduceRet] = None,
                 **kwargs: object) -> ReduceRet | RunRet | None:
        """Call a synchronous client method on every cluster member.

        Args:
            method_name: Name of the synchronous client method to invoke.
            *args: Positional arguments forwarded to each method call.
            reduce: Optional accumulator for every method result.
            initialize: Initial value required when ``reduce`` is provided.
            **kwargs: Keyword arguments forwarded to each method call.

        Returns:
            The last result, reduced result, or ``None`` for an empty cluster.

        Raises:
            ValueError: If a reducer is provided without an initial value.
        """
        if reduce is None:
            last: Optional[RunRet] = None
            for client in self.clients:
                method = cast(Callable[..., RunRet],
                              getattr(client, method_name))
                last = method(*args, **kwargs)
            return last

        if initialize is None:
            raise ValueError('initialize is required when reduce is provided')

        retval: ReduceRet = initialize
        for client in self.clients:
            method = cast(Callable[..., RunRet], getattr(client, method_name))
            ret = method(*args, **kwargs)
            retval = reduce(retval, ret)
        return retval

    def set_on_connected(self, func: OnConnectedFunc) -> None:
        """Set the connection callback on every client.

        Args:
            func: Callback to register on every client.
        """
        self.run_sync('set_on_connected', func)

    def set_on_disconnected(self, func: OnDisconnectedFunc) -> None:
        """Set the disconnection callback on every client.

        Args:
            func: Callback to register on every client.
        """
        self.run_sync('set_on_disconnected', func)

    def set_prefix(self, prefix: str) -> None:
        """Set the function-name prefix on every client.

        Args:
            prefix: Namespace prefix to apply before sending function names.
        """
        self.run_sync('set_prefix', prefix)

    def set_subfix(self, subfix: str) -> None:
        """Set the function-name suffix on every client.

        Args:
            subfix: Namespace suffix to apply before sending function names.
        """
        self.run_sync('set_subfix', subfix)

    async def connect(self, transports: Dict[str, BaseTransport]) -> None:
        """Connect every client using its configured entrypoint transport.

        Args:
            transports: Mapping from entrypoint to connection transport.

        Raises:
            Exception: If an entrypoint has no configured transport.
        """
        for entrypoint, client in zip(self.entrypoints, self.clients):
            transport = transports.get(entrypoint)
            if not transport:
                raise Exception(f'No transport for {entrypoint}')
            await client.connect(transport)

    def close(self) -> None:
        """Close every client connection in the cluster."""
        self.run_sync('close')

    async def submit_job(
        self,
        func: str | bytes = '',
        name: str | bytes = '',
        workload: bytes = b'',
        sched_at: int = 0,
        count: int = 0,
        timeout: int = 0,
        job: Optional[Job] = None,
    ) -> bool:
        """Submit a job to the server selected by its name hash.

        Args:
            func: Function name for a newly created job.
            name: Job name used for client selection.
            workload: Serialized job payload.
            sched_at: Unix timestamp at which to schedule the job.
            count: Number of times to run the job.
            timeout: Job timeout in seconds.
            job: Existing job object, which takes precedence over individual fields.

        Returns:
            Whether the selected server accepted the job.
        """
        if job is None:
            job = Job(func, name, workload, sched_at, count, timeout)
        client = self.get(to_str(job.name))
        return await client.submit_job(job=job)

    async def run_job(
        self,
        func: str | bytes = '',
        name: str | bytes = '',
        workload: bytes = b'',
        sched_at: int = 0,
        count: int = 0,
        timeout: int = 0,
        job: Optional[Job] = None,
        stream: Optional[RunJobStreamFunc] = None,
    ) -> bytes:
        """Run a job on the server selected by its name hash.

        Args:
            func: Function name for a newly created job.
            name: Job name used for client selection.
            workload: Serialized job payload.
            sched_at: Unix timestamp at which to schedule the job.
            count: Number of times to run the job.
            timeout: Job timeout in seconds.
            job: Existing job object, which takes precedence over individual fields.
            stream: Optional synchronous or asynchronous callback for job data.

        Returns:
            Final job response payload.
        """
        if job is None:
            job = Job(func, name, workload, sched_at, count, timeout)
        client = self.get(to_str(job.name))
        return await client.run_job(job=job, stream=stream)

    async def recv_job_data(
        self,
        func: str,
        name: str,
        timeout: int = 120,
        fut: Optional[asyncio.Future[bool]] = None,
    ) -> AsyncIterable[bytes]:
        """Yield job data from the server selected by the job name.

        Args:
            func: Function that owns the job.
            name: Job name used for client selection.
            timeout: Maximum seconds to await each stream response.
            fut: Optional future completed after the server success marker.

        Yields:
            Data chunks emitted by the job.
        """
        client = self.get(name)
        async for v in client.recv_job_data(func, name, timeout, fut):
            yield v

    async def remove_job(self, func: str, name: str) -> bool:
        """Remove one job from the server selected by its name hash.

        Args:
            func: Function that owns the job.
            name: Job name used for client selection.

        Returns:
            Whether the selected server removed the job.
        """
        client = self.get(name)
        return await client.remove_job(func, name)

    async def drop_func(self, func: str) -> None:
        """Remove a function from every server in the cluster.

        Args:
            func: Function name to drop.
        """
        await self.run('drop_func', func)

    async def status(self) -> StatusMap:
        """Return merged function status from every server in the cluster.

        Returns:
            Worker and job counts aggregated by function name.
        """
        stats: StatusMap = {}
        for client in self.clients:
            stat = await client.status()
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
