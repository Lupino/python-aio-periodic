"""Define worker responses consumed by the periodic job runtime."""

import json
from typing import Union

from .types.utils import Encodable, JSONValue


class DoneResponse:
    """Represent a successfully completed job.

    Args:
        buf: Optional payload returned to the job caller.
    """
    buf: Encodable

    def __init__(self, buf: Encodable = None) -> None:
        self.buf = buf


class FailResponse:
    """Represent a job that failed without scheduling a retry."""
    pass


class SchedLaterResponse:
    """Represent a job that should run again after a delay.

    Args:
        delay: Number of seconds to wait before the next run.
        count: Retry count associated with the rescheduled job.
    """
    delay: int
    count: int

    def __init__(self, delay: int, count: int = 0) -> None:
        self.delay = delay
        self.count = count


# Worker._process_job dispatches these marker objects to the corresponding
# protocol command. Other return values are treated as successful payloads.
ResponseTypes = Union[DoneResponse, FailResponse, SchedLaterResponse]


def done(buf: Encodable = None) -> DoneResponse:
    """Create a successful job response.

    Args:
        buf: Optional payload returned to the job caller.

    Returns:
        DoneResponse: A response marking the job as complete.
    """
    return DoneResponse(buf)


def jsonify(data: JSONValue) -> DoneResponse:
    """Create a successful response containing UTF-8 JSON data.

    Args:
        data: JSON-compatible data to encode as the response payload.

    Returns:
        DoneResponse: A response containing the encoded JSON payload.
    """
    # The Periodic protocol transports completion payloads as bytes rather
    # than Python objects, so JSON responses must be encoded before dispatch.
    return DoneResponse(json.dumps(data).encode('utf-8'))


def fail() -> FailResponse:
    """Create a failed job response.

    Returns:
        FailResponse: A response marking the job as failed.
    """
    return FailResponse()


def sched_later(delay: int, count: int = 0) -> SchedLaterResponse:
    """Create a response that reschedules a job.

    Args:
        delay: Number of seconds to wait before the next run.
        count: Retry count associated with the rescheduled job.

    Returns:
        SchedLaterResponse: A response carrying the retry schedule.
    """
    return SchedLaterResponse(delay, count)
