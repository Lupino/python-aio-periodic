"""Provide per-message request agents for periodic client communication."""

import asyncio
from binascii import crc32
from collections import deque
from typing import Deque, TYPE_CHECKING, Union, Optional, Type

from .utils import MAGIC_REQUEST, encode_int32, to_bytes
from .command import Command

if TYPE_CHECKING:
    from .base_client import BaseClient


class Agent(object):
    """Coordinate request and response data for one client message ID.

    The agent owns a bounded FIFO response buffer and the waiters consuming it.
    It is registered with ``BaseClient`` only for the lifetime of its context.

    Args:
        client: Client that owns this agent and its transport locks.
        timeout: Maximum seconds to wait for one response.
        autoid: Whether to allocate a unique message ID on entry. Disable this
            only for reserved protocol commands.
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
        """Initialize an unregistered agent.

        Args:
            client: Client that owns this agent.
            timeout: Maximum seconds to wait for a response.
            autoid: Whether entering the context allocates a message ID.
        """
        self.msgid = b''
        # Deques preserve FIFO delivery while keeping both buffer operations O(1).
        self._buffer = deque()
        self._waiters = deque()
        self.client = client
        self.timeout = timeout
        self.autoid = autoid

    def __enter__(self) -> 'Agent':
        """Register the agent with the client synchronously.

        Returns:
            The registered agent.
        """
        if self.autoid:
            self.msgid = self.client.get_next_msgid()
        else:
            # Reserved ID for special commands (e.g., status/ping)
            self.msgid = b'\xFF\xFF\xFF\x00'

        self.client.agents[self.msgid] = self
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: object,
    ) -> None:
        """Unregister the agent when its context exits.

        Args:
            exc_type: Exception type raised inside the context, if any.
            exc_val: Exception instance raised inside the context, if any.
            exc_tb: Traceback object for the exception, if any.
        """
        if self.msgid:
            self.client.agents.pop(self.msgid, None)

    async def __aenter__(self) -> 'Agent':
        """Register the agent while serializing message ID allocation.

        Returns:
            The registered agent.
        """
        async with self.client.msgid_locker:
            return self.__enter__()

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: object,
    ) -> None:
        """Unregister the agent while holding the message ID lock.

        Args:
            exc_type: Exception type raised inside the context, if any.
            exc_val: Exception instance raised inside the context, if any.
            exc_tb: Traceback object for the exception, if any.
        """
        async with self.client.msgid_locker:
            self.__exit__(exc_type, exc_val, exc_tb)

    def feed_data(self, data: bytes) -> None:
        """Queue incoming data and wake the oldest pending receiver.

        The buffer retains no more than the outstanding demand plus the newest
        response, preventing unsolicited data from accumulating indefinitely.

        Args:
            data: Decoded payload received for this agent's message ID.
        """
        waiter_count = len(self._waiters)

        # Retain the newest unsolicited response so callers may send before
        # they begin receiving, but discard older excess responses. This caps
        # the backlog at one item beyond the currently waiting consumers.
        if len(self._buffer) > waiter_count:
            self._buffer.pop()

        self._buffer.appendleft(data)

        if waiter_count > 0:
            # New waiters are added on the left, making the rightmost waiter
            # the earliest one and preserving FIFO wake-up order.
            waiter = self._waiters.pop()
            waiter.set()

    async def receive(self) -> bytes:
        """Return the next buffered response or wait for one.

        Returns:
            The oldest response payload available to this agent.

        Raises:
            asyncio.TimeoutError: If no response arrives before ``timeout``.
        """
        try:
            # Use wait_for for broader compatibility (vs asyncio.timeout)
            return await asyncio.wait_for(self._receive_internal(),
                                          timeout=self.timeout)
        except asyncio.TimeoutError:
            raise

    async def _receive_internal(self) -> bytes:
        """Wait for a response when the buffer is empty.

        Returns:
            The oldest buffered response payload.
        """
        if len(self._buffer) == 0:
            waiter = self._make_waiter()
            await waiter.wait()

        return self._buffer.pop()

    def buffer_len(self) -> int:
        """Return the number of buffered response payloads.

        Returns:
            Current response-buffer length.
        """
        return len(self._buffer)

    async def send(self,
                   cmd: Union[Command, bytes],
                   force: bool = False) -> None:
        """Encode and send a command through the client transport.

        Args:
            cmd: Protocol command or already-encoded command bytes.
            force: Whether to send without waiting for an active connection.

        Raises:
            Exception: Propagates a transport write or drain failure.
        """
        if not force:
            await self.client.connected_wait()

        async with self.client.send_locker:
            payload = bytes(cmd)
            if self.msgid:
                payload = self.msgid + payload

            # The wire format prefixes each payload with its magic, size, and CRC.
            size_bytes = encode_int32(len(payload))
            crc_val = crc32(to_bytes(payload))
            crc_bytes = encode_int32(crc_val)

            packet = MAGIC_REQUEST + size_bytes + crc_bytes + to_bytes(payload)

            writer = self.client.get_writer()
            try:
                writer.write(packet)
                await writer.drain()
            except Exception as e:
                # Mark disconnected and proactively kick reconnect loop on
                # transport write failures.
                self.client.connected_evt.clear()
                if not self.client._closing:
                    writer_obj = self.client._writer
                    if writer_obj:
                        writer_obj.close()
                    self.client.start_connect()
                raise e

    def _make_waiter(self) -> asyncio.Event:
        """Create and enqueue a receiver notification event.

        Returns:
            Event that is set when data is available for a waiting receiver.
        """
        waiter = asyncio.Event()
        self._waiters.appendleft(waiter)
        return waiter
