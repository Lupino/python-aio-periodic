import json as jsonLib


class DoneResponse(object):

    def __init__(self, buf=b''):
        self.buf = buf


class FailResponse(object):
    pass


class SchedLaterResponse(object):

    def __init__(self, delay, count=0):
        self.delay = delay
        self.count = count


def done(buf=b''):
    return DoneResponse(buf)


def json(data):
    return DoneResponse(bytes(jsonLib.dumps(data), 'utf-8'))


def fail():
    return FailResponse()


def sched_later(delay, count=0):
    return SchedLaterResponse(delay, count)
