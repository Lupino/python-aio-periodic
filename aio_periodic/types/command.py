"""Serialize Periodic Task System protocol commands sent by clients and workers."""

from typing import List, Tuple, Union
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
# 0x22 WC.JobUnassigned
JOB_UNASSIGNED = b'\x22'


class Command(object):
    """Combine an opcode and encoded command payload.

    Args:
        payload: A complete protocol payload or ordered payload parts to join.
    """
    _payload: bytes

    _parts_type = (list, tuple)

    def __init__(
        self,
        payload: Union[utils.Encodable, List[utils.Encodable],
                       Tuple[utils.Encodable, ...]],
    ) -> None:
        if isinstance(payload, self._parts_type):
            self._payload = b''.join([utils.to_bytes(p) for p in payload])
        else:
            self._payload = utils.to_bytes(payload)

    def __bytes__(self) -> bytes:
        """Return the serialized command payload.

        Returns:
            The complete protocol command bytes.
        """
        return self._payload


class Noop(Command):
    """Create a no-operation command."""

    def __init__(self) -> None:
        super().__init__(NOOP)


class GrabJob(Command):
    """Request the next available job from the server."""

    def __init__(self) -> None:
        super().__init__(GRAB_JOB)


class SchedLater(Command):
    """Return a job to the queue after the requested delay.

    Args:
        job_handle: The server-issued handle of the claimed job.
        delay: The delay in seconds before the job becomes eligible again.
        count: The retry count to record with the rescheduled job.
    """

    def __init__(self, job_handle: bytes, delay: int, count: int = 0) -> None:
        super().__init__((SCHED_LATER, job_handle, utils.encode_int64(delay),
                          utils.encode_int16(count)))


class WorkDone(Command):
    """Report successful completion of a claimed job.

    Args:
        job_handle: The server-issued handle of the claimed job.
        buf: Optional result data returned with the completion acknowledgement.
    """

    def __init__(
        self,
        job_handle: bytes,
        buf: utils.Encodable = None,
    ) -> None:
        super().__init__((WORK_DONE, job_handle, utils.to_bytes(buf)))


class WorkFail(Command):
    """Report failure for a claimed job.

    Args:
        job_handle: The server-issued handle of the claimed job.
    """

    def __init__(self, job_handle: bytes) -> None:
        super().__init__((WORK_FAIL, job_handle))


class CanDo(Command):
    """Advertise a function that the worker can execute.

    Args:
        func: The function name the worker accepts.
    """

    def __init__(self, func: str) -> None:
        super().__init__((CAN_DO, utils.encode_str8(func)))


class Broadcast(Command):
    """Request broadcast delivery for the named function.

    Args:
        func: The function name that selects broadcast recipients.
    """

    def __init__(self, func: str) -> None:
        super().__init__((BROADCAST, utils.encode_str8(func)))


class CantDo(Command):
    """Withdraw a worker capability for the named function.

    Args:
        func: The function name the worker no longer accepts.
    """

    def __init__(self, func: str) -> None:
        super().__init__((CANT_DO, utils.encode_str8(func)))


class SubmitJob(Command):
    """Submit a job through the client protocol.

    Args:
        job: The job definition to enqueue.
    """

    def __init__(self, job: Job) -> None:
        super().__init__((SUBMIT_JOB, bytes(job)))


class Status(Command):
    """Request queue status from the server."""

    def __init__(self) -> None:
        super().__init__(STATUS)


class DropFunc(Command):
    """Request removal of all queued jobs for a function.

    Args:
        func: The function whose queued jobs should be removed.
    """

    def __init__(self, func: str) -> None:
        super().__init__((DROP_FUNC, utils.encode_str8(func)))


class RemoveJob(Command):
    """Request removal of a named job from a function queue.

    Args:
        func: The function that owns the named job.
        name: The job name to remove.
    """

    def __init__(self, func: str, name: utils.Encodable) -> None:
        super().__init__((
            REMOVE_JOB,
            utils.encode_str8(func),
            utils.encode_str8(name),
        ))


class RunJob(Command):
    """Submit a job for immediate execution.

    Args:
        job: The job definition to run without queue scheduling.
    """

    def __init__(self, job: Job) -> None:
        super().__init__((RUN_JOB, bytes(job)))


class Acquire(Command):
    """Request acquisition of the named distributed lock.

    Args:
        name: The distributed lock name.
        count: The requested lock capacity.
        handle: The requester handle associated with the lock.
    """

    def __init__(self, name: utils.Encodable, count: int,
                 handle: bytes) -> None:
        super().__init__((ACQUIRE, utils.encode_str8(name),
                          utils.encode_int16(count), handle))


class Release(Command):
    """Release a previously acquired distributed lock.

    Args:
        name: The distributed lock name.
        handle: The requester handle that acquired the lock.
    """

    def __init__(self, name: utils.Encodable, handle: bytes) -> None:
        super().__init__((RELEASE, utils.encode_str8(name), handle))


class RecvData(Command):
    """Acknowledge receipt of streamed job data.

    Args:
        job: The streamed job data acknowledgement payload.
    """

    def __init__(self, job: Job) -> None:
        super().__init__((RECV_DATA, bytes(job)))


class WorkData(Command):
    """Send a streamed data chunk for a claimed job.

    Args:
        job_handle: The server-issued handle of the claimed job.
        buf: The data chunk to deliver.
    """

    def __init__(
        self,
        job_handle: bytes,
        buf: utils.Encodable = None,
    ) -> None:
        super().__init__((WORK_DATA, job_handle, utils.to_bytes(buf)))


class JobAssigned(Command):
    """Acknowledge that the worker accepted a job assignment."""

    def __init__(self) -> None:
        super().__init__(JOB_ASSIGNED)


class JobUnassigned(Command):
    """Acknowledge that the worker declined a job assignment."""

    def __init__(self) -> None:
        super().__init__(JOB_UNASSIGNED)
