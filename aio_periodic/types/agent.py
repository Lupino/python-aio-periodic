from .utils import MAGIC_REQUEST, encode_int32, to_bytes
from binascii import crc32
import asyncio
import async_timeout
from typing import List, Any, TYPE_CHECKING
from .command import Command

if TYPE_CHECKING:
    from .base_client import BaseClient


class Agent(object):
    msgid: bytes
    _buffer: List[bytes]
    _waiters: List[asyncio.Event]
    client: 'BaseClient'
    timeout: int
    autoid: bool

    def __init__(self,
                 client: 'BaseClient',
                 timeout: int = 10,
                 autoid: bool = True) -> None:
        self.msgid = b''
        self._buffer = []
        self._waiters = []
        self.client = client
        self.timeout = timeout
        self.autoid = autoid

    def __enter__(self) -> 'Agent':
        if self.autoid:
            self.msgid = self.client.get_next_msgid()
        else:
            self.msgid = b'\xFF\xFF\xFF\x00'
        self.client.agents[self.msgid] = self
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self.msgid:
            self.client.agents.pop(self.msgid, None)

    async def __aenter__(self) -> 'Agent':
        async with self.client.msgid_locker:
            return self.__enter__()

    async def __aexit__(self, exc_type: Any, exc_val: Any,
                        exc_tb: Any) -> None:
        async with self.client.msgid_locker:
            self.__exit__(exc_type, exc_val, exc_tb)

    def feed_data(self, data: bytes) -> None:
        waiter_count = len(self._waiters)
        if len(self._buffer) > waiter_count:
            self._buffer.pop()

        self._buffer.insert(0, data)
        if waiter_count > 0:
            waiter = self._waiters.pop()
            waiter.set()

    async def receive(self) -> bytes:
        async with async_timeout.timeout(self.timeout):
            if len(self._buffer) == 0:
                waiter = self._make_waiter()
                await waiter.wait()
            return self._buffer.pop()

    def buffer_len(self) -> int:
        return len(self._buffer)

    async def send(self, cmd: Command | bytes, force: bool = False) -> None:
        if not force:
            await self.client.connected_wait()
        async with self.client._send_locker:
            payload = bytes(cmd)
            if self.msgid:
                payload = self.msgid + payload
            size = encode_int32(len(payload))
            crc = encode_int32(crc32(to_bytes(payload)))
            writer = self.client.get_writer()
            try:
                writer.write(MAGIC_REQUEST + size + crc + to_bytes(payload))
                await writer.drain()
            except Exception as e:
                self.client.connected_evt.clear()
                raise e

    def _make_waiter(self) -> asyncio.Event:
        waiter = asyncio.Event()
        self._waiters.insert(0, waiter)
        return waiter
