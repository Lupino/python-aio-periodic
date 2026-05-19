import asyncio
import unittest
from binascii import crc32
from unittest.mock import AsyncMock, Mock

from aio_periodic.types.base_client import BaseClient, BaseCluster, StatusMap
from aio_periodic.worker import Worker
from aio_periodic.transport import BaseTransport
from aio_periodic.types.job import Job as JobPayload
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


class WorkerGrabLogicTests(unittest.IsolatedAsyncioTestCase):
    async def test_next_grab_skips_when_disconnected(self) -> None:
        worker = Worker([])
        worker.connected_evt.clear()

        grab_agent = AsyncMock()
        grab_agent.is_timeout.return_value = True
        worker.grab_queue = asyncio.Queue()
        await worker.grab_queue.put(grab_agent)
        worker._pool = type('Pool', (), {'is_full': False})()

        await asyncio.wait_for(worker.next_grab(), timeout=0.2)
        grab_agent.safe_send.assert_not_awaited()

    async def test_run_task_does_not_regrab_after_disconnect(self) -> None:
        worker = Worker([])
        worker.connected_evt.set()

        msgid = b'\x00\x00\x00\x01'
        grab_agent = AsyncMock()
        worker.grab_agents[msgid] = grab_agent

        async def fake_process(_: object) -> None:
            worker.connected_evt.clear()

        worker.process_job = fake_process  # type: ignore[method-assign]
        payload = b'\x05' + bytes(JobPayload('echo', 'demo'))

        await asyncio.wait_for(worker.run_task(payload, msgid), timeout=0.2)
        grab_agent.send_assigned.assert_awaited_once()
        grab_agent.safe_send.assert_not_awaited()

    async def test_run_task_unexpected_error_reports_fail(self) -> None:
        worker = Worker([])
        worker.connected_evt.set()

        msgid = b'\x00\x00\x00\x02'
        grab_agent = AsyncMock()
        worker.grab_agents[msgid] = grab_agent

        async def boom(_: object) -> None:
            raise RuntimeError('boom')

        worker.process_job = boom  # type: ignore[method-assign]
        worker.send_command_and_receive = AsyncMock(return_value=True)  # type: ignore[method-assign]
        payload = b'\x05' + bytes(JobPayload('echo', 'demo'))

        await asyncio.wait_for(worker.run_task(payload, msgid), timeout=0.2)

        self.assertGreaterEqual(worker.send_command_and_receive.await_count, 1)  # type: ignore[attr-defined]
        sent_cmd = worker.send_command_and_receive.await_args_list[0].args[0]  # type: ignore[attr-defined]
        self.assertEqual(bytes(sent_cmd)[0:1], b'\x04')
        grab_agent.send_assigned.assert_awaited_once()

    async def test_message_callback_unassigns_when_pool_full(self) -> None:
        worker = Worker([])
        worker.next_grab = AsyncMock()  # type: ignore[method-assign]
        worker.run_task = AsyncMock()  # type: ignore[method-assign]
        msgid = b'\x00\x00\x00\x01'
        grab_agent = AsyncMock()
        worker.grab_agents[msgid] = grab_agent

        pool = type('Pool', (), {})()
        pool.is_full = True
        pool.spawn_n = AsyncMock()
        worker._pool = pool  # type: ignore[assignment]

        await worker._message_callback(b'\x05payload', msgid)

        grab_agent.send_unassigned.assert_awaited_once()
        pool.spawn_n.assert_not_called()

    async def test_message_callback_unassigns_when_stopping(self) -> None:
        worker = Worker([])
        worker.next_grab = AsyncMock()  # type: ignore[method-assign]
        worker.run_task = AsyncMock()  # type: ignore[method-assign]
        worker._stopping = True
        msgid = b'\x00\x00\x00\x03'
        grab_agent = AsyncMock()
        worker.grab_agents[msgid] = grab_agent

        pool = type('Pool', (), {})()
        pool.is_full = False
        pool.spawn_n = AsyncMock()
        worker._pool = pool  # type: ignore[assignment]

        await worker._message_callback(b'\x05payload', msgid)

        grab_agent.send_unassigned.assert_awaited_once()
        pool.spawn_n.assert_not_called()


class WorkerGracefulShutdownTests(unittest.IsolatedAsyncioTestCase):
    async def test_graceful_shutdown_sends_cant_do_waits_and_closes(self) -> None:
        worker = Worker([])

        async def task(_: object) -> bytes:
            return b'ok'

        worker._tasks = {'echo': task, 'echo_later': task}
        worker.send_command_and_receive = AsyncMock(return_value=True)  # type: ignore[method-assign]
        worker.close = Mock()  # type: ignore[method-assign]

        pool = type('Pool', (), {})()
        pool.join = AsyncMock()
        worker._pool = pool  # type: ignore[assignment]

        await worker.graceful_shutdown()

        self.assertTrue(worker._stopping)
        self.assertEqual(worker.send_command_and_receive.await_count, 2)  # type: ignore[attr-defined]
        for call in worker.send_command_and_receive.await_args_list:  # type: ignore[attr-defined]
            self.assertEqual(bytes(call.args[0])[0:1], b'\x08')
        pool.join.assert_awaited_once()
        worker.close.assert_called_once()


class BaseClientStatusTests(unittest.IsolatedAsyncioTestCase):
    async def test_status_parses_payload_to_typed_map(self) -> None:
        client = BaseClient(TYPE_CLIENT)
        payload = (
            b"echo,2,3,1,0,100\n"
            b"task,1,4,2,1,90\n"
        )

        async def fake_send(command: object, parse: object, timeout: int = 10
                            ) -> StatusMap:
            del command, timeout
            parser = parse
            assert callable(parser)
            return parser(payload)

        client.send_command_and_receive = fake_send  # type: ignore[method-assign]
        stats = await client.status()

        self.assertEqual(set(stats.keys()), {"echo", "task"})
        self.assertEqual(stats["echo"]["worker_count"], 2)
        self.assertEqual(stats["echo"]["job_count"], 3)
        self.assertEqual(stats["echo"]["processing"], 1)
        self.assertEqual(stats["echo"]["locked"], 0)
        self.assertEqual(stats["echo"]["sched_at"], 100)

    async def test_cluster_status_merges_counts_and_min_sched_at(self) -> None:
        class StubClient:
            def __init__(self, stats: StatusMap) -> None:
                self._stats = stats

            async def status(self) -> StatusMap:
                return self._stats

        cluster = BaseCluster.__new__(BaseCluster)
        cluster.clients = [
            StubClient({
                "echo": {
                    "func_name": "echo",
                    "worker_count": 2,
                    "job_count": 3,
                    "processing": 1,
                    "locked": 0,
                    "sched_at": 100,
                },
                "task": {
                    "func_name": "task",
                    "worker_count": 1,
                    "job_count": 1,
                    "processing": 0,
                    "locked": 0,
                    "sched_at": 80,
                },
            }),
            StubClient({
                "echo": {
                    "func_name": "echo",
                    "worker_count": 3,
                    "job_count": 2,
                    "processing": 2,
                    "locked": 1,
                    "sched_at": 70,
                },
            }),
        ]

        merged = await cluster.status()
        self.assertEqual(merged["echo"]["worker_count"], 5)
        self.assertEqual(merged["echo"]["job_count"], 5)
        self.assertEqual(merged["echo"]["processing"], 3)
        self.assertEqual(merged["echo"]["sched_at"], 70)
        self.assertEqual(merged["task"]["worker_count"], 1)


if __name__ == '__main__':
    unittest.main()
