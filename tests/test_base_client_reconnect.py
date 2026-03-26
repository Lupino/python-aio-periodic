import asyncio
import unittest
from binascii import crc32
from unittest.mock import AsyncMock

from aio_periodic.types.base_client import BaseClient
from aio_periodic.worker import Worker
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


class FailingAfterFirstDrainWriter(FakeWriter):
    def __init__(self, reader: asyncio.StreamReader) -> None:
        super().__init__(reader)
        self._drain_calls = 0

    async def drain(self) -> None:
        self._drain_calls += 1
        if self._drain_calls > 1:
            raise ConnectionError('write failed')
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


class FailOnSecondWriteTransport(BaseTransport):
    def __init__(self) -> None:
        self.calls = 0

    async def get(self) -> tuple[asyncio.StreamReader, FakeWriter]:
        self.calls += 1
        reader = asyncio.StreamReader()
        payload = b'cid'
        packet = (MAGIC_RESPONSE + encode_int32(len(payload)) +
                  encode_int32(crc32(payload)) + payload)
        reader.feed_data(packet)
        writer = FailingAfterFirstDrainWriter(reader)
        return reader, writer


class FlakyTransport(BaseTransport):
    def __init__(self) -> None:
        self.calls = 0

    async def get(self) -> tuple[asyncio.StreamReader, FakeWriter]:
        self.calls += 1
        if self.calls == 2:
            raise ConnectionError('temporary reconnect failure')

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

    async def test_reconnect_retries_after_first_failure(self) -> None:
        transport = FlakyTransport()
        client = BaseClient(TYPE_CLIENT)
        await client.connect(transport)
        await asyncio.sleep(0.05)
        self.assertEqual(transport.calls, 1)

        client.close(reconnect=True)
        await asyncio.sleep(1.5)

        # Attempt 2 fails; attempt 3 should succeed.
        self.assertGreaterEqual(transport.calls, 3)
        self.assertTrue(client.connected)
        client.close()

    async def test_write_failure_triggers_reconnect(self) -> None:
        transport = FailOnSecondWriteTransport()
        client = BaseClient(TYPE_CLIENT)
        await client.connect(transport)
        await asyncio.sleep(0.05)
        self.assertEqual(transport.calls, 1)

        with self.assertRaises(ConnectionError):
            async with client.agent() as agent:
                await agent.send(b'\x01')

        await asyncio.sleep(0.5)
        self.assertGreaterEqual(transport.calls, 2)
        self.assertTrue(client.connected)
        client.close()


class WorkerReconnectTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.transport = FakeTransport()

    async def asyncTearDown(self) -> None:
        return None

    async def test_worker_auto_reregisters_funcs_after_reconnect(self) -> None:
        worker = Worker([])

        async def task(_: object) -> bytes:
            return b'ok'

        await worker.add_func('echo', task)
        worker._add_func = AsyncMock(return_value=True)  # type: ignore[method-assign]

        await worker.connect(self.transport)
        self.assertEqual(worker._add_func.await_count, 1)  # type: ignore[attr-defined]

        worker.get_writer().close()
        await asyncio.sleep(0.4)

        self.assertGreaterEqual(self.transport.calls, 2)
        self.assertGreaterEqual(worker._add_func.await_count, 2)  # type: ignore[attr-defined]
        worker.close()


if __name__ == '__main__':
    unittest.main()
