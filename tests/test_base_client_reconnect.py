import asyncio
import unittest
from binascii import crc32

from aio_periodic.types.base_client import BaseClient
from aio_periodic.transport import BaseTransport
from aio_periodic.types.utils import MAGIC_RESPONSE, TYPE_CLIENT, encode_int32


class FakeWriter:
    def __init__(self, reader: asyncio.StreamReader) -> None:
        self.reader = reader
        self.closed = False

    def write(self, data: bytes) -> None:
        del data

    async def drain(self) -> None:
        return None

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        self.reader.feed_eof()

    async def wait_closed(self) -> None:
        return None


class FakeTransport(BaseTransport):
    def __init__(self) -> None:
        self.calls = 0

    async def get(self) -> tuple[asyncio.StreamReader, FakeWriter]:
        self.calls += 1
        reader = asyncio.StreamReader()
        payload = b'cid'
        packet = (MAGIC_RESPONSE + encode_int32(len(payload)) +
                  encode_int32(crc32(payload)) + payload)
        reader.feed_data(packet)
        writer = FakeWriter(reader)
        return reader, writer


class BaseClientReconnectTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.transport = FakeTransport()

    async def asyncTearDown(self) -> None:
        return None

    async def test_explicit_close_does_not_reconnect(self) -> None:
        client = BaseClient(TYPE_CLIENT)
        await client.connect(self.transport)
        await asyncio.sleep(0.05)
        self.assertEqual(self.transport.calls, 1)

        client.close()
        await asyncio.sleep(0.3)

        self.assertEqual(self.transport.calls, 1)

    async def test_close_with_reconnect_reconnects(self) -> None:
        client = BaseClient(TYPE_CLIENT)
        await client.connect(self.transport)
        await asyncio.sleep(0.05)
        self.assertEqual(self.transport.calls, 1)

        client.close(reconnect=True)
        await asyncio.sleep(0.5)

        self.assertGreaterEqual(self.transport.calls, 2)
        client.close()


if __name__ == '__main__':
    unittest.main()
