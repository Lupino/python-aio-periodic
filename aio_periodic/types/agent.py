import asyncio
from binascii import crc32
from collections import deque
from typing import Any, Deque, TYPE_CHECKING, Union

from .utils import MAGIC_REQUEST, encode_int32, to_bytes
from .command import Command

if TYPE_CHECKING:
    from .base_client import BaseClient


class Agent(object):
    """
    Handles message sending and receiving for a specific message ID (msgid).
    Acts as a bridge between the BaseClient and specific job tasks.
    """
    msgid: bytes
    _buffer: Deque[bytes]
    _waiters: Deque[asyncio.Event]
    client: 'BaseClient'
    timeout: int
    autoid: bool

    def __init__(self,
                 client: 'BaseClient',
                 timeout: int = 10,
                 autoid: bool = True) -> None:
        self.msgid = b''
        # Use deque for O(1) appends and pops on both ends
        self._buffer = deque()
        self._waiters = deque()
        self.client = client
        self.timeout = timeout
        self.autoid = autoid

    def __enter__(self) -> 'Agent':
        """Registers the agent with the client synchronously."""
        if self.autoid:
            self.msgid = self.client.get_next_msgid()
        else:
            # Reserved ID for special commands (e.g., status/ping)
            self.msgid = b'\xFF\xFF\xFF\x00'

        self.client.agents[self.msgid] = self
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Unregisters the agent to prevent memory leaks."""
        if self.msgid:
            self.client.agents.pop(self.msgid, None)

    async def __aenter__(self) -> 'Agent':
        """
        Async context manager.
        Acquires the msgid_locker to ensure thread-safe ID generation.
        """
        async with self.client.msgid_locker:
            return self.__enter__()

    async def __aexit__(self, exc_type: Any, exc_val: Any,
                        exc_tb: Any) -> None:
        async with self.client.msgid_locker:
            self.__exit__(exc_type, exc_val, exc_tb)

    def feed_data(self, data: bytes) -> None:
        """
        Called by the client loop when data arrives for this agent.
        Implements a FIFO queue that drops the oldest data if the buffer
        exceeds the number of active waiters.
        """
        waiter_count = len(self._waiters)

        # Prevent buffer from growing indefinitely if no one is listening.
        # This keeps the buffer size roughly equal to the demand.
        if len(self._buffer) > waiter_count:
            self._buffer.pop()  # Remove the oldest item (right side)

        self._buffer.appendleft(data)  # Add new item to front (left side)

        if waiter_count > 0:
            waiter = self._waiters.pop()  # Wake up the longest-waiting task
            waiter.set()

    async def receive(self) -> bytes:
        """
        Waits for data to arrive in the buffer.
        Raises TimeoutError if data does not arrive within self.timeout.
        """
        try:
            # Use wait_for for broader compatibility (vs asyncio.timeout)
            return await asyncio.wait_for(self._receive_internal(),
                                          timeout=self.timeout)
        except asyncio.TimeoutError:
            raise

    async def _receive_internal(self) -> bytes:
        """Internal helper to handle the wait logic."""
        if len(self._buffer) == 0:
            waiter = self._make_waiter()
            await waiter.wait()

        return self._buffer.pop()

    def buffer_len(self) -> int:
        return len(self._buffer)

    async def send(self,
                   cmd: Union[Command, bytes],
                   force: bool = False) -> None:
        """
        Encodes and sends a command via the client's transport.
        """
        if not force:
            await self.client.connected_wait()

        async with self.client.send_locker:
            payload = bytes(cmd)
            if self.msgid:
                payload = self.msgid + payload

            # Prepare packet: Magic + Size + CRC + Payload
            size_bytes = encode_int32(len(payload))
            crc_val = crc32(to_bytes(payload))
            crc_bytes = encode_int32(crc_val)

            packet = MAGIC_REQUEST + size_bytes + crc_bytes + to_bytes(payload)

            writer = self.client.get_writer()
            try:
                writer.write(packet)
                await writer.drain()
            except Exception as e:
                # Force a reconnect on write failure
                self.client.connected_evt.clear()
                raise e

    def _make_waiter(self) -> asyncio.Event:
        """Creates a new event waiter and adds it to the queue."""
        waiter = asyncio.Event()
        self._waiters.appendleft(waiter)
        return waiter
