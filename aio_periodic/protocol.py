from . import utils
import asyncio


@asyncio.coroutine
def create_unix_connection(path=None, loop=None, **kwds):
    """Similar to `create_connection` but works with UNIX Domain Sockets."""
    if loop is None:
        loop = asyncio.get_event_loop()
    protocol = Protocol(loop=loop)
    transport, _ = yield from loop.create_unix_connection(
        lambda: protocol, path, **kwds)

    return transport, protocol


@asyncio.coroutine
def create_connection(host, port, loop=None, **kwds):
    if loop is None:
        loop = asyncio.get_event_loop()
    protocol = Protocol(loop=loop)
    transport, _ = yield from loop.create_connection(
        lambda: protocol, host, port, **kwds)

    return transport, protocol


def open(entrypoint):
    if entrypoint.startswith("unix://"):
        transport, protocol = yield from create_unix_connection(
            entrypoint.split("://")[1])
    else:
        host_port = entrypoint.split("://")[1].split(":")
        transport, protocol = yield from create_connection(host_port[0],
                                                           host_port[1])
    return transport, protocol


class Protocol(asyncio.FlowControlMixin, asyncio.Protocol):
    """Helper class to adapt between Protocol and StreamReader.

    (This is a helper class instead of making StreamReader itself a
    Protocol subclass, because the StreamReader has other potential
    uses, and to prevent the user of the StreamReader to accidentally
    call inappropriate methods of the protocol.)
    """

    def __init__(self, loop=None):
        super().__init__(loop=loop)
        self._stream_readers = {}
        self._transport = None
        self._buffer = bytearray()

    def connection_made(self, transport):
        self._transport = transport

    def connection_lost(self, exc):
        if exc is None:
            for reader in self._stream_readers.values():
                reader.fead_eof()
        else:
            for reader in self._stream_readers.values():
                reader.set_exception(exc)
        super().connection_lost(exc)

    def data_received(self, data):
        self._buffer.extend(data)
        if len(self._buffer) >= 4:
            length = utils.parseHeader(bytes(self._buffer[:4]))
            if len(self._buffer) >= 4 + length:
                data = bytes(self._buffer[4:4 + length])
                self._buffer = self._buffer[4 + length:]
                msgId = data.split(utils.NULL_CHAR, 2)[0]
                msgId = int(msgId)
                reader = self._stream_readers[msgId]
                reader.feed_data(data)

    def eof_received(self):
        for reader in self._stream_readers.values():
            reader.fead_eof()

    def set_reader(self, msgId, reader):
        self._stream_readers[msgId] = reader

    def remove_reader(self, msgId):
        self._stream_readers.pop(msgId)
