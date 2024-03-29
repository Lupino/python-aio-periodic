from . import utils
from .job import Job
from typing import Any

NOOP = b'\x00'
# for job
GRAB_JOB = b'\x01'
SCHED_LATER = b'\x02'
WORK_DONE = b'\x03'
WORK_FAIL = b'\x04'
JOB_ASSIGN = b'\x05'
NO_JOB = b'\x06'
# for func
CAN_DO = b'\x07'
BROADCAST = b'\x15'
CANT_DO = b'\x08'
# for test
PING = b'\x09'
PONG = b'\x0A'
# other
SLEEP = b'\x0B'
UNKNOWN = b'\x0C'
# client command
SUBMIT_JOB = b'\x0D'
STATUS = b'\x0E'
DROP_FUNC = b'\x0F'
REMOVE_JOB = b'\x11'

RUN_JOB = b'\x19'

ACQUIRED = b'\x1A'
ACQUIRE = b'\x1B'
RELEASE = b'\x1C'

NO_WORKER = b'\x1D'
DATA = b'\x1E'

SUCCESS = b'\x10'


class Command(object):
    _payload: bytes

    def __init__(self, payload: Any) -> None:
        if isinstance(payload, list):
            payload = b''.join([utils.to_bytes(p) for p in payload])
        elif isinstance(payload, str):
            payload = utils.to_bytes(payload)

        self._payload = payload

    def __bytes__(self) -> bytes:
        return self._payload


class Noop(Command):

    def __init__(self) -> None:
        Command.__init__(self, NOOP)


class GrabJob(Command):

    def __init__(self) -> None:
        Command.__init__(self, GRAB_JOB)


class SchedLater(Command):

    def __init__(self, job_handle: bytes, delay: int, count: int = 0) -> None:
        Command.__init__(self, [
            SCHED_LATER, job_handle,
            utils.encode_int64(delay),
            utils.encode_int16(count)
        ])


class WorkDone(Command):

    def __init__(self, job_handle: bytes, buf: Any = None) -> None:
        Command.__init__(self, [WORK_DONE, job_handle, utils.to_bytes(buf)])


class WorkFail(Command):

    def __init__(self, job_handle: bytes) -> None:
        Command.__init__(self, [WORK_FAIL, job_handle])


class CanDo(Command):

    def __init__(self, func: str) -> None:
        Command.__init__(self, [CAN_DO, utils.encode_str8(func)])


class Broadcast(Command):

    def __init__(self, func: str) -> None:
        Command.__init__(self, [BROADCAST, utils.encode_str8(func)])


class CantDo(Command):

    def __init__(self, func: str) -> None:
        Command.__init__(self, [CANT_DO, utils.encode_str8(func)])


class SubmitJob(Command):

    def __init__(self, job: Job) -> None:
        Command.__init__(self, [SUBMIT_JOB, bytes(job)])


class Status(Command):

    def __init__(self) -> None:
        Command.__init__(self, STATUS)


class DropFunc(Command):

    def __init__(self, func: str) -> None:
        Command.__init__(self, [DROP_FUNC, utils.encode_str8(func)])


class RemoveJob(Command):

    def __init__(self, func: str, name: Any) -> None:
        Command.__init__(
            self,
            [REMOVE_JOB,
             utils.encode_str8(func),
             utils.encode_str8(name)])


class RunJob(Command):

    def __init__(self, job: Job) -> None:
        Command.__init__(self, [RUN_JOB, bytes(job)])


class Acquire(Command):

    def __init__(self, name: Any, count: int, handle: Any) -> None:
        Command.__init__(self, [
            ACQUIRE,
            utils.encode_str8(name),
            utils.encode_int16(count), handle
        ])


class Release(Command):

    def __init__(self, name: Any, handle: Any) -> None:
        Command.__init__(self, [RELEASE, utils.encode_str8(name), handle])
