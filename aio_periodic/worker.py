from .job import Job
from . import utils
from .utils import create_agent
from .protocol import open


class Worker(object):
    def __init__(self):
        self.connected = False
        self._protocol = None
        self._transport = None
        self.agents = dict()
        self._msgId = 0

    def _connect(self):
        self._transport, self._protocol = yield from open(self._entryPoint)

        if self._transport:
            try:
                self._transport.close()
            except Exception:
                pass
        self._msgId = 0
        agent = create_agent(self._transport, self._protocol, self._msgId)
        yield from agent.send(utils.TYPE_WORKER)
        self.connected = True
        return True

    def add_server(self, entryPoint):
        self._entryPoint = entryPoint

    @property
    def agent(self):
        self._msgId += 1
        agent = create_agent(self._transport, self._protocol, self._msgId)
        return agent

    def connect(self):
        try:
            ret = yield from self.ping()
            if ret:
                self.connected = True
                return True
        except Exception:
            pass

        print("Try to reconnecting %s"%(self._entryPoint))
        connected = yield from self._connect()
        return connected

    def ping(self):
        yield from self.agent.send([utils.PING])
        payload = yield from self.agent.recive()
        if payload == utils.PONG:
            return True
        return False

    def grabJob(self):
        yield from self.agent.send([utils.GRAB_JOB])
        payload = yield from self.agent.recive()
        if payload == utils.NO_JOB or payload == utils.WAIT_JOB:
            return None

        return Job(payload, self.agent)

    def add_func(self, func):
        yield from self.agent.send([utils.CAN_DO, func])

    def remove_func(self, func):
        yield from self.agent.send([utils.CANT_DO, func])

    def close(self):
        self.agent.close()
