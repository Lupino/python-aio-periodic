import asyncio
from .utils import MAGIC_REQUEST, encode_int32, to_bytes
from binascii import crc32

class Agent(object):
    def __init__(self, writer, msgid, loop=None):
        self._writer = writer
        self.msgid = msgid
        self._buffer = []
        self._loop = loop
        self._waiters = []

    def feed_data(self, data):
        self._buffer.insert(0, data)
        if len(self._waiters) > 0:
            waiter = self._waiters.pop()
            waiter.set_result(True)

    async def receive(self):
        if len(self._buffer) == 0:
            waiter = self._make_waiter()
            await waiter
        return self._buffer.pop()

    def buffer_len(self):
        return len(self._buffer)

    async def send(self, cmd):
        payload = bytes(cmd)
        if self.msgid:
            payload = self.msgid + payload
        size = encode_int32(len(payload))
        crc = encode_int32(crc32(to_bytes(payload)))
        self._writer.write(MAGIC_REQUEST + size + crc + to_bytes(payload))
        await self._writer.drain()

    def _make_waiter(self):
        waiter = asyncio.create_future(loop=self._loop)
        self._waiters.insert(0, waiter)
        return waiter
