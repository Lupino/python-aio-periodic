import json
from typing import Union

from .types.utils import Encodable, JSONValue


class DoneResponse:
    """
    Indicates that the job finished successfully.
    Optionally carries a payload (buf) back to the caller.
    """
    buf: Encodable

    def __init__(self, buf: Encodable = None) -> None:
        self.buf = buf


class FailResponse:
    """
    Indicates that the job failed execution.
    """
    pass


class SchedLaterResponse:
    """
    Indicates that the job should be retried or rescheduled later.
    """
    delay: int
    count: int

    def __init__(self, delay: int, count: int = 0) -> None:
        self.delay = delay
        self.count = count


# Type alias for valid return types from a worker function
ResponseTypes = Union[DoneResponse, FailResponse, SchedLaterResponse]


def done(buf: Encodable = None) -> DoneResponse:
    """Helper to create a DoneResponse."""
    return DoneResponse(buf)


def jsonify(data: JSONValue) -> DoneResponse:
    """Helper to create a DoneResponse with JSON-encoded data."""
    return DoneResponse(json.dumps(data).encode('utf-8'))


def fail() -> FailResponse:
    """Helper to create a FailResponse."""
    return FailResponse()


def sched_later(delay: int, count: int = 0) -> SchedLaterResponse:
    """Helper to create a SchedLaterResponse."""
    return SchedLaterResponse(delay, count)
