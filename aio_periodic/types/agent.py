import asyncio
from .utils import MAGIC_REQUEST, encode_str32

class Agent(object):
    def __init__(self, writer, msgid, loop=None):
        self._writer = writer
        self.msgid = msgid
        self._buffer = []
        self._loop = loop
        self._waiter = None

    def feed_data(self, data):
        self._buffer.append(data)
        if self._waiter:
            self._waiter.set_result(True)

    @asyncio.coroutine
    def recive(self):
        if len(self._buffer) == 0:
            waiter = self._make_waiter()
            yield from waiter
        return self._buffer.pop()

    def buffer_len(self):
        return len(self._buffer)

    @asyncio.coroutine
    def send(self, cmd):
        payload = bytes(cmd)
        if self.msgid:
            payload = msgid + payload
        self._writer.write(MAGIC_REQUEST + encode_str32(payload))
        yield from self._writer.drain()

    def _make_waiter(self):
        waiter = self._waiter
        assert waiter is None or waiter.cancelled()
        waiter = asyncio.Future(loop=self._loop)
        self._waiter = waiter
        return waiter
