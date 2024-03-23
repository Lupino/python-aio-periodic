from .types.utils import encode_str8, to_str
from .types.job import Job as JobT
from .types import command as cmd
from .types.base_client import is_success
import json
from typing import Any, TYPE_CHECKING, Callable, cast, Coroutine

if TYPE_CHECKING:
    from .worker import Worker


class FinishedError(Exception):
    pass


class Job(object):
    payload: JobT
    job_handle: bytes
    w: 'Worker'
    finished: bool

    def __init__(self, payload: bytes, w: 'Worker') -> None:

        self.payload = JobT.build(payload)

        self.job_handle = encode_str8(self.payload.func) + encode_str8(
            self.payload.name)

        self.w = w

        self.finished = False

    def _check_finished(self) -> None:
        if self.finished:
            raise FinishedError('Job is already finished')

        self.finished = True

    async def done(self, buf: bytes = b'') -> bool:
        self._check_finished()
        return cast(
            bool, await
            self.w.send_command_and_receive(cmd.WorkDone(self.job_handle, buf),
                                            is_success))

    async def sched_later(self, delay: int, count: int = 0) -> bool:
        self._check_finished()
        return cast(
            bool, await self.w.send_command_and_receive(
                cmd.SchedLater(self.job_handle, delay, count), is_success))

    async def fail(self) -> bool:
        self._check_finished()
        return cast(
            bool, await
            self.w.send_command_and_receive(cmd.WorkFail(self.job_handle),
                                            is_success))

    async def acquire(self, name: str, count: int) -> bool:

        def parse(payload: bytes) -> bool:
            if payload[0:1] == cmd.ACQUIRED:
                if payload[1] == 1:
                    return True

            self.finished = True
            return False

        command = cmd.Acquire(name, count, self.job_handle)
        return cast(bool, await
                    self.w.send_command_and_receive(command, parse))

    async def release(self, name: str) -> bool:
        return cast(
            bool, await
            self.w.send_command_and_receive(cmd.Release(name, self.job_handle),
                                            is_success))

    async def with_lock(self,
                        name: str,
                        count: int,
                        task: Callable[[], Coroutine[Any, Any, Any]],
                        release: bool = False) -> None:
        acquired = await self.acquire(name, count)
        if acquired:
            await task()
            if release:
                await self.release(name)

    @property
    def func_name(self) -> str:
        return self.w._strip_prefix_subfix(to_str(self.payload.func))

    @property
    def name(self) -> str:
        return to_str(self.payload.name)

    @property
    def sched_at(self) -> int:
        return self.payload.sched_at

    @property
    def timeout(self) -> int:
        return self.payload.timeout

    @property
    def workload(self) -> bytes:
        return self.payload.workload

    @property
    def workload_json(self) -> Any:
        return json.loads(to_str(self.payload.workload))
