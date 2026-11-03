import asyncio
import unittest
from binascii import crc32
from contextlib import suppress
from unittest.mock import AsyncMock, Mock

from aio_periodic.types.base_client import BaseClient, BaseCluster, StatusMap
from aio_periodic.worker import Worker
from aio_periodic.transport import BaseTransport, Transport
from aio_periodic.types import command as cmd
from aio_periodic.types.job import Job as JobPayload
from aio_periodic.types.utils import (MAGIC_RESPONSE, TYPE_CLIENT,
                                      TYPE_WORKER, build_client_registration,
                                      encode_int32)


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

    async def test_manual_reconnect_does_not_start_another_supervisor(
        self,
    ) -> None:
        class SlowTransport(FakeTransport):
            async def get(self):
                if self.calls >= 1:
                    await asyncio.sleep(0.02)
                return await super().get()

        transport = SlowTransport()
        client = BaseClient(TYPE_CLIENT)
        try:
            await client.connect(transport)
            await asyncio.sleep(0)
            await client.connect()
            await asyncio.sleep(0.05)
            self.assertEqual(transport.calls, 2)
            self.assertIsNone(client._reconnect_task)
            self.assertEqual(len(client._processes), 2)
        finally:
            client.close()

    async def test_manual_connect_cancels_pending_automatic_connect(
        self,
    ) -> None:
        entered = asyncio.Event()
        release = asyncio.Event()
        cancelled = asyncio.Event()

        class PausedTransport(FakeTransport):
            attempts = 0

            async def get(self):
                self.attempts += 1
                if self.attempts == 2:
                    entered.set()
                    try:
                        await release.wait()
                    except asyncio.CancelledError:
                        cancelled.set()
                        raise
                return await super().get()

        transport = PausedTransport()
        client = BaseClient(TYPE_CLIENT)
        try:
            await client.connect(transport)
            await asyncio.sleep(0)
            client.get_writer().close()
            await asyncio.wait_for(entered.wait(), timeout=1)
            await client.connect()
            replacement = client.get_writer()
            release.set()
            await asyncio.sleep(0.05)
            self.assertTrue(cancelled.is_set())
            self.assertIs(client.get_writer(), replacement)
            self.assertEqual(transport.calls, 2)
        finally:
            release.set()
            client.close()

    async def test_late_write_failure_does_not_close_replacement(self) -> None:
        entered = asyncio.Event()
        release = asyncio.Event()
        client = BaseClient(TYPE_CLIENT)
        pending = None
        connecting = None
        try:
            await client.connect(self.transport)
            previous = client.get_writer()

            async def late_failure() -> None:
                entered.set()
                await release.wait()
                raise ConnectionError('old connection write failed')

            previous.drain = late_failure
            pending = asyncio.create_task(client.send_command(cmd.Status()))
            await entered.wait()
            connecting = asyncio.create_task(client.connect())
            await asyncio.sleep(0)
            replacement = client.get_writer()
            release.set()
            with self.assertRaises(ConnectionError):
                await asyncio.wait_for(pending, timeout=1)
            await asyncio.wait_for(connecting, timeout=1)
            self.assertTrue(client.connected)
            self.assertFalse(replacement.closed)
            self.assertIsNone(client._reconnect_task)
        finally:
            release.set()
            client.close()
            if pending and not pending.done():
                pending.cancel()
            if connecting and not connecting.done():
                connecting.cancel()

    async def test_close_during_dial_does_not_reopen_connection(self) -> None:
        entered = asyncio.Event()
        release = asyncio.Event()

        class PausedTransport(FakeTransport):
            async def get(self):
                if self.calls >= 1:
                    entered.set()
                    await release.wait()
                return await super().get()

        transport = PausedTransport()
        client = BaseClient(TYPE_CLIENT)
        pending = None
        try:
            await client.connect(transport)
            pending = asyncio.create_task(client.connect())
            await entered.wait()
            client.close()
            release.set()
            with self.assertRaises(ConnectionError):
                await pending
            self.assertFalse(client.connected)
            self.assertTrue(client._closing)
            self.assertFalse(client._processes)
        finally:
            release.set()
            client.close()
            if pending and not pending.done():
                pending.cancel()

    async def test_queued_old_command_cannot_precede_new_handshake(self) -> None:
        client = BaseClient(TYPE_CLIENT)
        pending = None
        connecting = None
        packets = []
        try:
            await client.connect(self.transport)
            await client.send_locker.acquire()
            pending = asyncio.create_task(client.send_command(cmd.Status()))
            await asyncio.sleep(0)
            connecting = asyncio.create_task(client.connect())
            await asyncio.sleep(0)
            client.get_writer().write = packets.append
            client.send_locker.release()
            with self.assertRaises(ConnectionError):
                await asyncio.wait_for(pending, timeout=1)
            await asyncio.wait_for(connecting, timeout=1)
            self.assertEqual([packet[12:] for packet in packets],
                             [TYPE_CLIENT])
            self.assertTrue(client.connected)
        finally:
            if client.send_locker.locked():
                client.send_locker.release()
            client.close()
            for task in (pending, connecting):
                if task and not task.done():
                    task.cancel()

    async def test_close_during_callback_cannot_report_connected(self) -> None:
        client = BaseClient(TYPE_CLIENT)
        entered = asyncio.Event()
        release = asyncio.Event()

        async def callback() -> None:
            entered.set()
            await release.wait()

        client.set_on_connected(callback)
        pending = asyncio.create_task(client.connect(self.transport))
        try:
            await entered.wait()
            client.close()
            release.set()
            with self.assertRaises(ConnectionError):
                await asyncio.wait_for(pending, timeout=1)
            self.assertFalse(client.connected)
        finally:
            release.set()
            client.close()
            if not pending.done():
                pending.cancel()

    async def test_cancelled_connect_releases_partial_connection(self) -> None:
        client = BaseClient(TYPE_CLIENT)
        entered = asyncio.Event()

        async def callback() -> None:
            entered.set()
            await asyncio.Event().wait()

        client.set_on_connected(callback)
        pending = asyncio.create_task(client.connect(self.transport))
        try:
            await entered.wait()
            writer = client.get_writer()
            pending.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await pending
            self.assertTrue(writer.closed)
            self.assertFalse(client.connected)
            self.assertFalse(client._processes)
        finally:
            client.close()
            if not pending.done():
                pending.cancel()

    async def test_explicit_close_does_not_reconnect(self) -> None:
        client = BaseClient(TYPE_CLIENT)
        await client.connect(self.transport)
        await asyncio.sleep(0.05)
        self.assertEqual(self.transport.calls, 1)

        client.close()
        await asyncio.sleep(0.3)

        self.assertEqual(self.transport.calls, 1)

    async def test_immediate_close_with_reconnect_reconnects(self) -> None:
        client = BaseClient(TYPE_CLIENT)
        try:
            await client.connect(self.transport)
            # Do not yield to the receive task before requesting reconnect.
            client.close(reconnect=True)
            await asyncio.sleep(0.1)
            self.assertGreaterEqual(self.transport.calls, 2)
        finally:
            client.close()

    async def test_explicit_close_remains_closed_after_receive_exits(
        self,
    ) -> None:
        client = BaseClient(TYPE_CLIENT)
        await client.connect(self.transport)
        await asyncio.sleep(0)
        client.close()
        await asyncio.sleep(0.02)
        self.assertTrue(client._closing)
        self.assertFalse(client.connected)
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

    async def test_disconnect_during_connect_callback_is_not_success(
        self,
    ) -> None:
        recovered = asyncio.Event()
        client = BaseClient(TYPE_CLIENT)

        async def on_connected() -> None:
            if self.transport.calls == 2:
                client.get_writer().close()
                await asyncio.sleep(0.02)
            elif self.transport.calls >= 3:
                recovered.set()

        client.set_on_connected(on_connected)
        try:
            await client.connect(self.transport)
            client.get_writer().close()
            await asyncio.wait_for(recovered.wait(), timeout=3)
            self.assertTrue(client.connected)
        finally:
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

    async def test_retiring_supervisor_keeps_replacement_reference(
        self,
    ) -> None:
        client = BaseClient(TYPE_CLIENT)
        started = asyncio.Queue()
        release = asyncio.Event()

        async def connect() -> bool:
            await started.put(True)
            await release.wait()
            return True

        client.connect = connect
        try:
            client.start_connect()
            await started.get()
            previous = client._reconnect_task
            client.close(reconnect=True)
            replacement = client._reconnect_task
            await started.get()
            self.assertIsNot(previous, replacement)
            self.assertIs(client._reconnect_task, replacement)
        finally:
            client.close()

    async def test_disconnect_does_not_return_empty_successful_response(
        self,
    ) -> None:
        client = BaseClient(TYPE_CLIENT)
        pending = None
        try:
            await client.connect(self.transport)
            pending = asyncio.create_task(client.send_command_and_receive(
                cmd.Status()))
            await asyncio.sleep(0)
            client.get_writer().close()
            with self.assertRaises(ConnectionError):
                await asyncio.wait_for(pending, timeout=1)
        finally:
            client.close()
            if pending and not pending.done():
                pending.cancel()

    async def test_empty_job_result_is_still_valid(self) -> None:
        client = BaseClient(TYPE_CLIENT)
        pending = None
        try:
            await client.connect(self.transport)
            pending = asyncio.create_task(client.run_job('echo', 'empty'))
            await asyncio.sleep(0)
            agent = next(iter(client.agents.values()))
            agent.feed_data(cmd.DATA)
            self.assertEqual(await pending, b'')
        finally:
            client.close()
            if pending and not pending.done():
                pending.cancel()


class ClientRegistrationTests(unittest.TestCase):
    def test_build_client_registration(self) -> None:
        self.assertEqual(build_client_registration(TYPE_CLIENT).hex(), '01')
        self.assertEqual(build_client_registration(TYPE_WORKER).hex(), '02')
        self.assertEqual(
            build_client_registration(TYPE_CLIENT, 'abc', 'def').hex(),
            '0300000000000000036162630000000000000003646566',
        )
        self.assertEqual(
            build_client_registration(TYPE_WORKER, 'abc', 'def').hex(),
            '0400000000000000036162630000000000000003646566',
        )

    def test_build_client_registration_rejects_partial_auth(self) -> None:
        with self.assertRaises(ValueError):
            build_client_registration(TYPE_CLIENT, 'abc')


class WorkerReconnectTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.transport = FakeTransport()

    async def asyncTearDown(self) -> None:
        return None

    async def test_tcp_disconnects_during_registration_restore_all_funcs(
        self,
    ) -> None:
        worker = Worker(['echo', 'notice'])
        worker.set_prefix('test_')
        connections = 0
        registrations = {}
        writers = []
        handlers = set()
        restored = asyncio.Event()
        completed = asyncio.Event()
        executions = []
        assigned = False
        work_task = None

        def frame(payload: bytes) -> bytes:
            return (MAGIC_RESPONSE + encode_int32(len(payload)) +
                    encode_int32(crc32(payload)) + payload)

        async def read_packet(reader: asyncio.StreamReader) -> bytes:
            header = await reader.readexactly(12)
            return await reader.readexactly(int.from_bytes(header[4:8], 'big'))

        async def handle(reader, writer) -> None:
            nonlocal connections, assigned
            current = asyncio.current_task()
            handlers.add(current)
            writers.append(writer)
            connections += 1
            connection = connections
            registrations[connection] = set()
            try:
                await read_packet(reader)
                writer.write(frame(b'connection-id'))
                await writer.drain()
                while True:
                    payload = await read_packet(reader)
                    opcode = payload[4:5]
                    if opcode in (cmd.CAN_DO, cmd.BROADCAST):
                        # Drop two successive connections without acknowledging
                        # CANDO, exercising real pending response agents.
                        if connection in (2, 3):
                            return
                        name = payload[6:6 + payload[5]].decode()
                        registrations[connection].add((opcode, name))
                        writer.write(frame(payload[:4] + cmd.SUCCESS))
                        await writer.drain()
                        if connection >= 4 and len(
                                registrations[connection]) == 2:
                            restored.set()
                    elif opcode == cmd.GRAB_JOB:
                        if connection >= 4 and not assigned:
                            assigned = True
                            job = JobPayload('test_echo', 'recovered-job')
                            writer.write(frame(
                                payload[:4] + cmd.JOB_ASSIGN + bytes(job)))
                        else:
                            writer.write(frame(payload[:4] + cmd.NO_JOB))
                        await writer.drain()
                    elif opcode == cmd.WORK_DONE:
                        writer.write(frame(payload[:4] + cmd.SUCCESS))
                        await writer.drain()
                        completed.set()
            except (asyncio.IncompleteReadError, ConnectionError):
                pass
            finally:
                writer.close()
                with suppress(ConnectionError):
                    await writer.wait_closed()
                handlers.discard(current)

        async def task(_: object) -> bytes:
            executions.append('echo')
            return b'ok'

        await worker.add_func('echo', task)
        await worker.broadcast('notice', task)
        await worker.add_func('disabled', task)
        server = await asyncio.start_server(handle, '127.0.0.1', 0)
        port = server.sockets[0].getsockname()[1]
        try:
            await worker.connect(Transport(f'tcp://127.0.0.1:{port}'))
            work_task = asyncio.create_task(worker.work(2))
            await asyncio.sleep(0)
            worker.close(reconnect=True)
            await asyncio.wait_for(restored.wait(), timeout=5)
            if worker._reconnect_task:
                await asyncio.wait_for(
                    asyncio.shield(worker._reconnect_task), timeout=2)
            self.assertTrue(worker.connected)
            self.assertEqual(registrations[4], {
                (cmd.CAN_DO, 'test_echo'),
                (cmd.BROADCAST, 'test_notice'),
            })
            self.assertEqual(registrations[1], registrations[4])
            await asyncio.wait_for(completed.wait(), timeout=3)
            await worker._pool.join()
            self.assertEqual(executions, ['echo'])
        finally:
            worker.stop()
            if work_task:
                await asyncio.wait_for(work_task, timeout=2)
            worker.close()
            server.close()
            await server.wait_closed()
            for writer in writers:
                writer.close()
            if handlers:
                await asyncio.gather(*list(handlers))

    async def test_disconnect_during_reregistration_retries_connection(
        self,
    ) -> None:
        worker = Worker([])
        registered = asyncio.Event()

        async def task(_: object) -> bytes:
            return b"ok"

        async def register(func: str) -> bool:
            del func
            if self.transport.calls == 2:
                worker.get_writer().close()
                # Let the receive loop observe EOF while connect() is still
                # awaiting registration on this connection.
                await asyncio.sleep(0.02)
                return False
            if self.transport.calls >= 3:
                registered.set()
            return True

        await worker.add_func("echo", task)
        worker._add_func = register
        try:
            await worker.connect(self.transport)
            worker.get_writer().close()
            await asyncio.wait_for(registered.wait(), timeout=3)
            self.assertGreaterEqual(self.transport.calls, 3)
        finally:
            worker.close()

    async def test_disconnect_during_registration_retry_delay(self) -> None:
        worker = Worker([])
        retrying = asyncio.Event()
        recovered = asyncio.Event()

        async def task(_: object) -> bytes:
            return b"ok"

        async def register(func: str) -> bool:
            del func
            await worker.connected_wait()
            if self.transport.calls == 2:
                retrying.set()
                return False
            if self.transport.calls >= 3:
                recovered.set()
            return True

        await worker.add_func("echo", task)
        worker._add_func = register
        try:
            await worker.connect(self.transport)
            worker.get_writer().close()
            await asyncio.wait_for(retrying.wait(), timeout=1)
            worker.get_writer().close()
            await asyncio.wait_for(recovered.wait(), timeout=4)
        finally:
            worker.close()

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
    async def test_graceful_shutdown_sends_cant_do_waits_and_closes(
        self,
    ) -> None:
        worker = Worker([])

        async def task(_: object) -> bytes:
            return b'ok'

        worker._tasks = {'echo': task, 'echo_later': task}
        worker.connected_evt.set()
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
