from .utils import create_agent
from . import utils
from .protocol import open
import json

class Client(object):
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
        yield from agent.send(utils.TYPE_CLIENT)
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
        payload = yield from self._agent.recive()
        if payload == utils.PONG:
            return True
        return False

    def submitJob(self, job):
        yield from self.agent.send([utils.SUBMIT_JOB, json.dumps(job)])
        payload = yield from self._agent.recive()
        if payload == utils.SUCCESS:
            return True
        else:
            return False

    def status(self):
        yield from self.agent.send([utils.STATUS])
        payload = yield from self._agent.recive()
        return json.loads(str(payload, "utf-8"))

    def dropFunc(self, func):
        yield from self.agent.send([utils.DROP_FUNC, func])
        payload = yield from self._agent.recive()
        if payload == utils.SUCCESS:
            return True
        else:
            return False

    def close(self):
        self._agent.close()
