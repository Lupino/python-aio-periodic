"""Provide a worker-side wrapper for a leased periodic job."""

from .types.utils import encode_str8, to_str
from .types.job import Job as JobT
from .types import command as cmd
from .types.base_client import is_success
import json
from typing import TYPE_CHECKING, Callable, cast, Optional, Awaitable, \
    TypeVar
from .types.utils import Encodable, JSONValue

if TYPE_CHECKING:
    from .worker import Worker

WithLockReturn = TypeVar('WithLockReturn')


class FinishedError(Exception):
    """Raise when an operation is attempted after a job is finalized."""

    pass


class Job(object):
    """Represent a job leased by a worker.

    A job can be finalized once through ``done``, ``fail``, or
    ``sched_later``. Sending intermediate data does not finalize the lease.
    """

    payload: JobT
    job_handle: bytes
    w: 'Worker'
    finished: bool

    def __init__(self, payload: bytes, w: 'Worker') -> None:
        """Build a job wrapper from its encoded payload.

        Args:
            payload: Encoded job data supplied by the periodic server.
            w: Worker that owns the connection for this job.
        """

        self.payload = JobT.build(payload)

        self.job_handle = encode_str8(self.payload.func) + encode_str8(
            self.payload.name)

        self.w = w

        self.finished = False

    def _check_finished(self, auto_finished: bool = True) -> None:
        """Validate that the job is active and optionally finalize it.

        Args:
            auto_finished: Whether the current operation consumes the job.

        Raises:
            FinishedError: If the job was already finalized.
        """
        if self.finished:
            raise FinishedError('Job is already finished')

        if auto_finished:
            self.finished = True

    async def done(self, buf: Encodable = None) -> bool:
        """Mark the job complete and optionally return encoded result data.

        Args:
            buf: Optional result payload sent with the completion command.

        Returns:
            Whether the periodic server accepted the completion command.
        """
        self._check_finished()
        return cast(
            bool, await
            self.w.send_command_and_receive(cmd.WorkDone(self.job_handle, buf),
                                            is_success))

    async def data(self, buf: Encodable = None) -> bool:
        """Send intermediate data without finalizing the job.

        Args:
            buf: Optional progress payload sent to the periodic server.

        Returns:
            Whether the periodic server accepted the data command.
        """
        self._check_finished(False)
        return cast(
            bool, await
            self.w.send_command_and_receive(cmd.WorkData(self.job_handle, buf),
                                            is_success))

    async def sched_later(self, delay: int, count: int = 0) -> bool:
        """Finalize the current lease and schedule its next execution.

        Args:
            delay: Delay in seconds before the next execution.
            count: Optional remaining execution count.

        Returns:
            Whether the periodic server accepted the reschedule command.
        """
        self._check_finished()
        return cast(
            bool, await self.w.send_command_and_receive(
                cmd.SchedLater(self.job_handle, delay, count), is_success))

    async def fail(self) -> bool:
        """Mark the job as failed.

        Returns:
            Whether the periodic server accepted the failure command.
        """
        self._check_finished()
        return cast(
            bool, await
            self.w.send_command_and_receive(cmd.WorkFail(self.job_handle),
                                            is_success))

    async def acquire(self, name: str, count: int) -> bool:
        """Acquire a named distributed lock for this job.

        A failed acquisition finalizes this job because another worker owns
        the requested concurrency slot.

        Args:
            name: Distributed lock name.
            count: Maximum number of concurrent lock holders.

        Returns:
            Whether the lock was acquired.
        """

        def parse(payload: bytes) -> bool:
            if payload[0:1] == cmd.ACQUIRED:
                if payload[1] == 1:
                    return True

            # The server will not redeliver this lease after lock contention.
            self.finished = True
            return False

        command = cmd.Acquire(name, count, self.job_handle)
        return cast(bool, await
                    self.w.send_command_and_receive(command, parse))

    async def release(self, name: str) -> bool:
        """Release a distributed lock held by this job.

        Args:
            name: Distributed lock name.

        Returns:
            Whether the periodic server accepted the release command.
        """
        return cast(
            bool, await
            self.w.send_command_and_receive(cmd.Release(name, self.job_handle),
                                            is_success))

    async def with_lock(self,
                        name: str,
                        count: int,
                        task: Callable[[], Awaitable[WithLockReturn]],
                        release: bool = False) -> Optional[WithLockReturn]:
        """Run a task after acquiring a named distributed lock.

        Args:
            name: Distributed lock name.
            count: Maximum number of concurrent lock holders.
            task: Awaitable callback to run after acquiring the lock.
            release: Whether to release the lock after the callback completes.

        Returns:
            The callback result, or ``None`` when the lock was unavailable.
        """
        acquired = await self.acquire(name, count)
        if acquired:
            try:
                return await task()
            finally:
                if release:
                    await self.release(name)

        return None

    @property
    def func_name(self) -> str:
        """Return the registered function name without worker name markers."""
        return self.w._strip_prefix_subfix(to_str(self.payload.func))

    @property
    def name(self) -> str:
        """Return the job name supplied by the periodic server."""
        return to_str(self.payload.name)

    @property
    def sched_at(self) -> int:
        """Return the scheduled Unix timestamp for this execution."""
        return self.payload.sched_at

    @property
    def timeout(self) -> int:
        """Return the execution timeout in seconds."""
        return self.payload.timeout

    @property
    def workload(self) -> bytes:
        """Return the raw workload payload."""
        return self.payload.workload

    @property
    def workload_json(self) -> JSONValue:
        """Decode and return the JSON workload payload."""
        return cast(JSONValue, json.loads(to_str(self.payload.workload)))
