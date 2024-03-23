import json
from typing import Any


class DoneResponse(object):
    buf: bytes

    def __init__(self, buf: bytes = b'') -> None:
        self.buf = buf


class FailResponse(object):
    pass


class SchedLaterResponse(object):
    delay: int
    count: int

    def __init__(self, delay: int, count: int = 0) -> None:
        self.delay = delay
        self.count = count


def done(buf: bytes = b'') -> DoneResponse:
    return DoneResponse(buf)


def jsonify(data: Any) -> DoneResponse:
    return DoneResponse(bytes(json.dumps(data), 'utf-8'))


def fail() -> FailResponse:
    return FailResponse()


def sched_later(delay: int, count: int = 0) -> SchedLaterResponse:
    return SchedLaterResponse(delay, count)
