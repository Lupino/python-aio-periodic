from .utils import MAGIC_REQUEST, encode_int32, to_bytes
from binascii import crc32
import asyncio
import async_timeout


class Agent(object):

    def __init__(self, client, timeout=10, autoid=True):
        self.msgid = None
        self._buffer = []
        self._waiters = []
        self.client = client
        self.timeout = timeout
        self.autoid = autoid

    def __enter__(self):
        if self.autoid:
            self.msgid = self.client.get_next_msgid()
        else:
            self.msgid = b'\xFF\xFF\xFF\x00'
        self.client.agents[self.msgid] = self
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.msgid:
            self.client.agents.pop(self.msgid, None)

    async def __aenter__(self):
        async with self.client.msgid_locker:
            return self.__enter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        async with self.client.msgid_locker:
            self.__exit__(exc_type, exc_val, exc_tb)

    def feed_data(self, data):
        waiter_count = len(self._waiters)
        if len(self._buffer) > waiter_count:
            self._buffer.pop()

        self._buffer.insert(0, data)
        if waiter_count > 0:
            waiter = self._waiters.pop()
            waiter.set()

    async def receive(self):
        async with async_timeout.timeout(self.timeout):
            if len(self._buffer) == 0:
                waiter = self._make_waiter()
                await waiter.wait()
            return self._buffer.pop()

    def buffer_len(self):
        return len(self._buffer)

    async def send(self, cmd, force=False):
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

    def _make_waiter(self):
        waiter = asyncio.Event()
        self._waiters.insert(0, waiter)
        return waiter
