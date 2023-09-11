import json


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


def jsonify(data):
    return DoneResponse(bytes(json.dumps(data), 'utf-8'))


def fail():
    return FailResponse()


def sched_later(delay, count=0):
    return SchedLaterResponse(delay, count)
