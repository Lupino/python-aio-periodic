from .utils import MAGIC_REQUEST, encode_int32, to_bytes
from binascii import crc32
from time import time
import asyncio


class Agent(object):
    def __init__(self, client, msgid):
        self.msgid = msgid
        self._buffer = []
        self._waiters = []
        self.client = client

    def feed_data(self, data):
        self._buffer.insert(0, data)
        if len(self._waiters) > 0:
            waiter = self._waiters.pop()
            waiter.set()

    async def receive(self):
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
            self.client._send_timer = time()
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
