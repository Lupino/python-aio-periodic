import asyncio
from .utils import MAGIC_REQUEST, encode_int32, to_bytes
from binascii import crc32


class Agent(object):
    def __init__(self, client, msgid, loop=None):
        self.msgid = msgid
        self._buffer = []
        self._loop = loop
        self._waiters = []
        self.client = client

    def feed_data(self, data):
        self._buffer.insert(0, data)
        if len(self._waiters) > 0:
            waiter = self._waiters.pop()
            if not waiter.cancelled():
                waiter.set_result(True)

    async def receive(self):
        if len(self._buffer) == 0:
            waiter = self._make_waiter()
            await waiter
        return self._buffer.pop()

    def buffer_len(self):
        return len(self._buffer)

    async def send(self, cmd):
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
                self.client.connected = False
                raise e

    def _make_waiter(self):
        waiter = self._loop.create_future()
        self._waiters.insert(0, waiter)
        return waiter
