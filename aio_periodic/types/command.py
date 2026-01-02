from . import utils
from .job import Job
from typing import Any

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
# 0x09 WC.Ping
# 0x09 CC.Ping
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
# 0x12 CC.Dump
# 0x13 CC.Load
# 0x14 CC.Shutdown
# 0x15 WC.Broadcast
BROADCAST = b'\x15'
# 0x16 CC.ConfigGet
# 0x17 CC.ConfigSet
# 0x18 SC.Config
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
# 0x20 WC.JobAssigned
JOB_ASSIGNED = b'\x21'


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


class RecvData(Command):

    def __init__(self, job: Job) -> None:
        Command.__init__(self, [RECV_DATA, bytes(job)])


class WorkData(Command):

    def __init__(self, job_handle: bytes, buf: Any = None) -> None:
        Command.__init__(self, [WORK_DATA, job_handle, utils.to_bytes(buf)])


class JobAssigned(Command):

    def __init__(self) -> None:
        Command.__init__(self, JOB_ASSIGNED)
