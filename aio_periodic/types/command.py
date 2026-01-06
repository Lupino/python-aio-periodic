from typing import Any, Union, List
from . import utils
from .job import Job

# --- Constants ---

# 0x00 SC.Noop
NOOP = b'\x00'
# 0x01 WC.GrabJob
GRAB_JOB = b'\x01'
# 0x02 WC.SchedLater
SCHED_LATER = b'\x02'
# 0x03 WC.WorkDone
WORK_DONE = b'\x03'
# 0x04 WC.WorkFail
WORK_FAIL = b'\x04'
# 0x05 SC.JobAssign
JOB_ASSIGN = b'\x05'
# 0x06 SC.NoJob
NO_JOB = b'\x06'
# 0x07 WC.CanDo
CAN_DO = b'\x07'
# 0x08 WC.CantDo
CANT_DO = b'\x08'
# 0x09 WC.Ping / CC.Ping
PING = b'\x09'
# 0x0A SC.Pong
PONG = b'\x0A'
# 0x0B WC.Sleep
SLEEP = b'\x0B'
# 0x0C SC.Unknown
UNKNOWN = b'\x0C'
# 0x0D CC.SubmitJob
SUBMIT_JOB = b'\x0D'
# 0x0E CC.Status
STATUS = b'\x0E'
# 0x0F CC.DropFunc
DROP_FUNC = b'\x0F'
# 0x10 SC.Success
SUCCESS = b'\x10'
# 0x11 CC.RemoveJob
REMOVE_JOB = b'\x11'
# 0x12 CC.Dump (Reserved)
# 0x13 CC.Load (Reserved)
# 0x14 CC.Shutdown (Reserved)
# 0x15 WC.Broadcast
BROADCAST = b'\x15'
# 0x16 CC.ConfigGet (Reserved)
# 0x17 CC.ConfigSet (Reserved)
# 0x18 SC.Config (Reserved)
# 0x19 CC.RunJob
RUN_JOB = b'\x19'
# 0x1A SC.Acquired
ACQUIRED = b'\x1A'
# 0x1B WC.Acquire
ACQUIRE = b'\x1B'
# 0x1C WC.Release
RELEASE = b'\x1C'
# 0x1D SC.NoWorker
NO_WORKER = b'\x1D'
# 0x1E SC.Data
DATA = b'\x1E'
# 0x1F CC.RecvData
RECV_DATA = b'\x1F'
# 0x20 WC.WorkData
WORK_DATA = b'\x20'
# 0x21 WC.JobAssigned
JOB_ASSIGNED = b'\x21'


class Command(object):
    """Base class for all network commands."""
    _payload: bytes

    def __init__(self, payload: Union[bytes, str, List[Any]]) -> None:
        if isinstance(payload, list):
            self._payload = b''.join([utils.to_bytes(p) for p in payload])
        else:
            self._payload = utils.to_bytes(payload)

    def __bytes__(self) -> bytes:
        return self._payload


class Noop(Command):

    def __init__(self) -> None:
        super().__init__(NOOP)


class GrabJob(Command):

    def __init__(self) -> None:
        super().__init__(GRAB_JOB)


class SchedLater(Command):

    def __init__(self, job_handle: bytes, delay: int, count: int = 0) -> None:
        super().__init__([
            SCHED_LATER, job_handle,
            utils.encode_int64(delay),
            utils.encode_int16(count)
        ])


class WorkDone(Command):

    def __init__(self, job_handle: bytes, buf: Any = None) -> None:
        super().__init__([WORK_DONE, job_handle, utils.to_bytes(buf)])


class WorkFail(Command):

    def __init__(self, job_handle: bytes) -> None:
        super().__init__([WORK_FAIL, job_handle])


class CanDo(Command):

    def __init__(self, func: str) -> None:
        super().__init__([CAN_DO, utils.encode_str8(func)])


class Broadcast(Command):

    def __init__(self, func: str) -> None:
        super().__init__([BROADCAST, utils.encode_str8(func)])


class CantDo(Command):

    def __init__(self, func: str) -> None:
        super().__init__([CANT_DO, utils.encode_str8(func)])


class SubmitJob(Command):

    def __init__(self, job: Job) -> None:
        super().__init__([SUBMIT_JOB, bytes(job)])


class Status(Command):

    def __init__(self) -> None:
        super().__init__(STATUS)


class DropFunc(Command):

    def __init__(self, func: str) -> None:
        super().__init__([DROP_FUNC, utils.encode_str8(func)])


class RemoveJob(Command):

    def __init__(self, func: str, name: Any) -> None:
        super().__init__(
            [REMOVE_JOB,
             utils.encode_str8(func),
             utils.encode_str8(name)])


class RunJob(Command):

    def __init__(self, job: Job) -> None:
        super().__init__([RUN_JOB, bytes(job)])


class Acquire(Command):

    def __init__(self, name: Any, count: int, handle: Any) -> None:
        super().__init__([
            ACQUIRE,
            utils.encode_str8(name),
            utils.encode_int16(count), handle
        ])


class Release(Command):

    def __init__(self, name: Any, handle: Any) -> None:
        super().__init__([RELEASE, utils.encode_str8(name), handle])


class RecvData(Command):

    def __init__(self, job: Job) -> None:
        super().__init__([RECV_DATA, bytes(job)])


class WorkData(Command):

    def __init__(self, job_handle: bytes, buf: Any = None) -> None:
        super().__init__([WORK_DATA, job_handle, utils.to_bytes(buf)])


class JobAssigned(Command):

    def __init__(self) -> None:
        super().__init__(JOB_ASSIGNED)
